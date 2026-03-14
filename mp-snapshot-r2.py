#!/usr/bin/env python3
"""
Mana Pool Snapshot Collector → R2

Fetches raw API responses and uploads them to a Cloudflare R2 bucket.
Processing happens later once enough snapshots have accumulated.

Pulls the FULL catalog including sales data for every card using
concurrent requests to /products/singles.

Run daily via cron:
  0 6 * * * cd /path/to/dir && python3 mp_snapshot_r2.py

Dependencies:
  pip install boto3 requests

Env vars:
  R2_ACCOUNT_ID        - Cloudflare account ID
  R2_ACCESS_KEY_ID     - R2 API token access key
  R2_SECRET_ACCESS_KEY - R2 API token secret key
  R2_BUCKET            - Bucket name (default: manapool-snapshots)
"""

import boto3
import requests
import json
import gzip
import sys
import time
import argparse
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL = "https://manapool.com/api/v1"
BATCH_SIZE = 100          # max scryfall_ids per /products/singles call
DEFAULT_CONCURRENCY = 10  # parallel requests
REQUEST_TIMEOUT = 120
MAX_RETRIES = 3
RETRY_BACKOFF = 2.0       # seconds, multiplied by attempt number

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("mp_snapshot")

# ---------------------------------------------------------------------------
# R2
# ---------------------------------------------------------------------------

def get_r2_client():
    account_id = os.environ["R2_ACCOUNT_ID"]
    return boto3.client(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )


def upload_to_r2(client, bucket: str, key: str, data: bytes, content_type: str = "application/json"):
    compressed = gzip.compress(data)
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=compressed,
        ContentType=content_type,
        ContentEncoding="gzip",
    )
    ratio = len(compressed) / len(data) * 100 if data else 0
    log.info(f"Uploaded {key} — {len(data):,} → {len(compressed):,} bytes ({ratio:.0f}%)")


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json"})


def fetch_json(url: str, params: dict | None = None, label: str = "") -> tuple[dict | None, float]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            t0 = time.time()
            resp = SESSION.get(url, params=params, timeout=REQUEST_TIMEOUT)
            elapsed = time.time() - t0
        except requests.RequestException as e:
            log.warning(f"[{label}] Attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF * attempt)
                continue
            log.error(f"[{label}] All retries exhausted")
            return None, 0

        if resp.status_code == 429:
            retry_after = float(resp.headers.get("Retry-After", RETRY_BACKOFF * attempt))
            log.warning(f"[{label}] Rate limited, sleeping {retry_after}s")
            time.sleep(retry_after)
            continue

        if resp.status_code != 200:
            log.error(f"[{label}] HTTP {resp.status_code}: {resp.text[:300]}")
            return None, 0

        try:
            data = resp.json()
            log.info(f"[{label}] OK — {len(resp.content):,} bytes, {elapsed:.2f}s")
            return data, elapsed
        except json.JSONDecodeError:
            log.error(f"[{label}] JSON parse error")
            return None, 0

    return None, 0


def fetch_products_batch(batch_info: tuple[int, int, list[str]]) -> tuple[int, list[dict]]:
    """Fetch a single batch. Returns (batch_index, products)."""
    idx, total, scryfall_ids = batch_info
    data, _ = fetch_json(
        f"{BASE_URL}/products/singles",
        params={"scryfall_ids": scryfall_ids},
        label=f"products {idx+1}/{total}",
    )
    if data and "data" in data:
        return idx, data["data"]
    return idx, []


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Mana Pool snapshot → R2")
    parser.add_argument("--bucket", type=str, default=os.environ.get("R2_BUCKET", "manapool-snapshots"))
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY,
                        help="Max parallel requests for sales data (default: 10)")
    parser.add_argument("--catalog-only", action="store_true", help="Skip sales collection")
    parser.add_argument("--dry-run", action="store_true", help="Fetch but don't upload")
    args = parser.parse_args()

    snapshot_ts = datetime.now(timezone.utc)
    date_str = snapshot_ts.strftime("%Y-%m-%d")
    ts_str = snapshot_ts.strftime("%Y%m%dT%H%M%SZ")
    prefix = f"snapshots/{date_str}/{ts_str}"

    log.info(f"Snapshot: {ts_str}")
    log.info(f"R2 prefix: {prefix}")
    log.info(f"Bucket: {args.bucket}")
    log.info(f"Concurrency: {args.concurrency}")

    if not args.dry_run:
        r2 = get_r2_client()
    else:
        r2 = None
        log.info("DRY RUN — fetch only, no upload")

    # -------------------------------------------------------------------
    # 1. Singles catalog
    # -------------------------------------------------------------------
    log.info("=" * 60)
    log.info("PHASE 1: Singles catalog")
    log.info("=" * 60)

    singles_data, singles_time = fetch_json(f"{BASE_URL}/prices/singles", label="prices/singles")
    if not singles_data or "data" not in singles_data:
        log.error("Failed to fetch singles — aborting")
        sys.exit(1)

    singles_list = singles_data["data"]
    log.info(f"Singles: {len(singles_list):,} cards")

    singles_bytes = json.dumps(singles_data).encode()
    if r2:
        upload_to_r2(r2, args.bucket, f"{prefix}/prices_singles.json.gz", singles_bytes)

    # -------------------------------------------------------------------
    # 2. Sealed catalog
    # -------------------------------------------------------------------
    log.info("=" * 60)
    log.info("PHASE 2: Sealed catalog")
    log.info("=" * 60)

    sealed_data, sealed_time = fetch_json(f"{BASE_URL}/prices/sealed", label="prices/sealed")
    if sealed_data:
        sealed_bytes = json.dumps(sealed_data).encode()
        if r2:
            upload_to_r2(r2, args.bucket, f"{prefix}/prices_sealed.json.gz", sealed_bytes)

    # -------------------------------------------------------------------
    # 3. Full sales data (concurrent)
    # -------------------------------------------------------------------
    all_products = []
    sales_time = 0
    total_batches = 0
    failed = 0
    total_sale_records = 0
    total_variants = 0

    if not args.catalog_only:
        log.info("=" * 60)
        log.info("PHASE 3: Sales data (full catalog, concurrent)")
        log.info("=" * 60)

        all_ids = [s["scryfall_id"] for s in singles_list if s.get("scryfall_id")]
        log.info(f"Cards with scryfall_id: {len(all_ids):,}")

        batches = [all_ids[i:i + BATCH_SIZE] for i in range(0, len(all_ids), BATCH_SIZE)]
        total_batches = len(batches)
        batch_args = [(i, total_batches, b) for i, b in enumerate(batches)]
        log.info(f"Batches: {total_batches} × {BATCH_SIZE} IDs, {args.concurrency} workers")

        t0 = time.time()
        completed = 0

        with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
            futures = {executor.submit(fetch_products_batch, ba): ba[0] for ba in batch_args}

            for future in as_completed(futures):
                idx, products = future.result()
                if products:
                    all_products.extend(products)
                else:
                    failed += 1
                completed += 1

                if completed % 50 == 0 or completed == total_batches:
                    elapsed = time.time() - t0
                    rate = completed / elapsed if elapsed > 0 else 0
                    eta = (total_batches - completed) / rate if rate > 0 else 0
                    log.info(f"Progress: {completed}/{total_batches} batches, "
                             f"{len(all_products):,} cards, "
                             f"{rate:.1f} batch/s, ETA {eta:.0f}s")

        sales_time = time.time() - t0

        total_sale_records = sum(
            len(v.get("recent_sales", []))
            for card in all_products
            for v in card.get("variants", [])
        )
        total_variants = sum(len(card.get("variants", [])) for card in all_products)

        log.info(f"Done: {len(all_products):,} cards, {total_variants:,} variants, "
                 f"{total_sale_records:,} sales in {sales_time:.1f}s "
                 f"({failed} failed batches)")

        products_payload = {
            "meta": {
                "snapshot_ts": snapshot_ts.isoformat(),
                "cards_requested": len(all_ids),
                "cards_returned": len(all_products),
                "batches_total": total_batches,
                "batches_failed": failed,
                "concurrency": args.concurrency,
                "fetch_duration_s": round(sales_time, 2),
            },
            "data": all_products,
        }

        products_bytes = json.dumps(products_payload).encode()
        if r2:
            upload_to_r2(r2, args.bucket, f"{prefix}/products_singles.json.gz", products_bytes)

    # -------------------------------------------------------------------
    # 4. Manifest
    # -------------------------------------------------------------------
    manifest = {
        "snapshot_ts": snapshot_ts.isoformat(),
        "prefix": prefix,
        "files": {
            "prices_singles": f"{prefix}/prices_singles.json.gz",
            "prices_sealed": f"{prefix}/prices_sealed.json.gz",
        },
        "stats": {
            "singles_count": len(singles_list),
            "singles_total_qty": sum(s.get("available_quantity", 0) for s in singles_list),
            "sealed_count": len(sealed_data.get("data", [])) if sealed_data else 0,
            "sealed_total_qty": sum(
                s.get("available_quantity", 0) for s in sealed_data.get("data", [])
            ) if sealed_data else 0,
            "singles_fetch_s": round(singles_time, 2),
            "sealed_fetch_s": round(sealed_time, 2),
        },
    }

    if not args.catalog_only:
        manifest["files"]["products_singles"] = f"{prefix}/products_singles.json.gz"
        manifest["stats"]["sales_fetch_s"] = round(sales_time, 2)
        manifest["stats"]["sales_concurrency"] = args.concurrency
        manifest["stats"]["cards_with_sales_data"] = len(all_products)
        manifest["stats"]["total_variants"] = total_variants
        manifest["stats"]["total_sale_records"] = total_sale_records
        manifest["stats"]["batches_total"] = total_batches
        manifest["stats"]["batches_failed"] = failed

    manifest_bytes = json.dumps(manifest, indent=2).encode()
    if r2:
        upload_to_r2(r2, args.bucket, f"{prefix}/manifest.json.gz", manifest_bytes)
        upload_to_r2(r2, args.bucket, "latest.json.gz", manifest_bytes)

    log.info("=" * 60)
    log.info("DONE")
    log.info("=" * 60)
    log.info(json.dumps(manifest["stats"], indent=2))


if __name__ == "__main__":
    main()