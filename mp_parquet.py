#!/usr/bin/env python3
"""Shared Parquet helpers for Mana Pool snapshot ingestion and querying."""

from __future__ import annotations

import gzip
import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.parquet as pq


FILE_TAG = "mp_parquet.py"
PARQUET_ROOT = "parquet"
PARQUET_CONTENT_TYPE = "application/vnd.apache.parquet"


log = logging.getLogger("mana_pool_parquet")
_DUCKDB_HTTPFS_INSTALLED = False


def _log_workflow(message: str) -> None:
    """Keep logs traceable to this module during backfills and ingestion runs."""
    log.info(f"{FILE_TAG}: {message}")


def _now_iso() -> str:
    """Capture a consistent ingestion timestamp for provenance columns."""
    return datetime.now(timezone.utc).isoformat()


def _normalize_snapshot_partition(snapshot_ts: str) -> str:
    """Convert ISO timestamps to folder-safe snapshot partition values."""
    parsed = datetime.fromisoformat(snapshot_ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    return parsed.strftime("%Y%m%dT%H%M%SZ")


def _snapshot_provenance(
    manifest: dict[str, Any],
    source_key: str,
    *,
    ingested_at: str | None = None,
) -> dict[str, Any]:
    """Build the shared provenance columns attached to every bronze and silver row."""
    snapshot_ts = manifest["snapshot_ts"]
    prefix = manifest["prefix"]
    snapshot_date = prefix.split("/")[1]
    return {
        "_snapshot_date": snapshot_date,
        "_snapshot_ts_partition": _normalize_snapshot_partition(snapshot_ts),
        "_snapshot_prefix": prefix,
        "_source_key": source_key,
        "_ingested_at": ingested_at or _now_iso(),
    }


def _sale_date(created_at: str | None) -> str | None:
    """Extract the canonical sale partition date from a sale timestamp when present."""
    value = str(created_at or "").strip()
    if len(value) < 10 or value[4:5] != "-" or value[7:8] != "-":
        return None
    return value[:10]


def sales_partition_prefix(sale_date: str) -> str:
    """Build the nested year/month/day prefix for canonical sales partitions."""
    parts = sale_date.split("-")
    if len(parts) != 3:
        raise ValueError(f"Invalid sale_date partition value: {sale_date!r}")
    year, month, day = parts
    return (
        f"{PARQUET_ROOT}/silver/sales_events/"
        f"sale_year={year}/sale_month={month}/sale_day={day}/"
    )


def _sale_identity(variant: dict[str, Any], sale: dict[str, Any]) -> str:
    """Use `created_at` as the canonical sale event id across carried-over snapshots."""
    return str(sale.get("created_at") or "")


def _json_string(value: Any) -> str:
    """Store the original row payload as compact JSON for source verification."""
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def decode_json_bytes(raw: bytes) -> dict[str, Any]:
    """Decode JSON payloads while tolerating mislabeled gzip/plain objects."""
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw.decode("utf-8"))


def read_json_object(client, bucket: str, key: str) -> dict[str, Any]:
    """Read a JSON object from R2 with gzip auto-detection."""
    _log_workflow(f"Reading JSON object {key}")
    obj = client.get_object(Bucket=bucket, Key=key)
    return decode_json_bytes(obj["Body"].read())


def put_json_object(
    client,
    bucket: str,
    key: str,
    payload: dict[str, Any],
    *,
    gzip_enabled: bool = True,
) -> None:
    """Write JSON back to R2, optionally gzip-encoding the payload."""
    raw = json.dumps(payload, indent=2).encode("utf-8")
    body = gzip.compress(raw) if gzip_enabled else raw
    kwargs = {
        "Bucket": bucket,
        "Key": key,
        "Body": body,
        # Storing gzip bytes directly keeps `.json.gz` objects self-describing on readback.
        "ContentType": "application/gzip" if gzip_enabled else "application/json",
    }
    _log_workflow(f"Writing JSON object {key}")
    client.put_object(**kwargs)


def list_object_keys(client, bucket: str, prefix: str) -> list[str]:
    """List all object keys under a prefix."""
    keys: list[str] = []
    continuation_token = None
    while True:
        params = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token:
            params["ContinuationToken"] = continuation_token
        response = client.list_objects_v2(**params)
        keys.extend(item["Key"] for item in response.get("Contents", []))
        if not response.get("IsTruncated"):
            break
        continuation_token = response.get("NextContinuationToken")
    return keys


def iter_manifest_keys(client, bucket: str) -> Iterable[str]:
    """Yield manifest keys in the raw snapshot archive."""
    for key in list_object_keys(client, bucket, "snapshots/"):
        if key.endswith("/manifest.json.gz"):
            yield key


def list_snapshot_manifests(client, bucket: str) -> list[dict[str, Any]]:
    """Load and sort all raw snapshot manifests from R2."""
    manifests = [read_json_object(client, bucket, key) for key in iter_manifest_keys(client, bucket)]
    manifests.sort(key=lambda item: item["snapshot_ts"])
    _log_workflow(f"Loaded {len(manifests)} manifests from raw snapshots")
    return manifests


def get_snapshot_source_keys(manifest: dict[str, Any]) -> dict[str, str]:
    """Resolve the raw object keys that belong to a manifest."""
    files = dict(manifest.get("files", {}))
    files["manifest"] = f"{manifest['prefix']}/manifest.json.gz"
    return files


def _recent_sale_type() -> pa.DataType:
    """Shared nested sale type used by bronze and silver schemas."""
    return pa.list_(
        pa.struct(
            [
                pa.field("created_at", pa.string()),
                pa.field("price", pa.int64()),
                pa.field("quantity", pa.int64()),
            ]
        )
    )


def _variant_type() -> pa.DataType:
    """Shared nested variant type for preserving raw card payloads."""
    return pa.list_(
        pa.struct(
            [
                pa.field("product_type", pa.string()),
                pa.field("product_id", pa.string()),
                pa.field("tcgplayer_sku_id", pa.int64()),
                pa.field("language_id", pa.string()),
                pa.field("condition_id", pa.string()),
                pa.field("finish_id", pa.string()),
                pa.field("low_price", pa.int64()),
                pa.field("available_quantity", pa.int64()),
                pa.field("recent_sales", _recent_sale_type()),
            ]
        )
    )


def manifest_schema() -> pa.Schema:
    """Schema for bronze manifest rows."""
    return pa.schema(
        [
            pa.field("_snapshot_date", pa.string()),
            pa.field("_snapshot_ts_partition", pa.string()),
            pa.field("_snapshot_prefix", pa.string()),
            pa.field("_source_key", pa.string()),
            pa.field("_ingested_at", pa.string()),
            pa.field("snapshot_ts", pa.string()),
            pa.field("prefix", pa.string()),
            pa.field(
                "files",
                pa.struct(
                    [
                        pa.field("prices_singles", pa.string()),
                        pa.field("prices_sealed", pa.string()),
                        pa.field("products_singles", pa.string()),
                    ]
                ),
            ),
            pa.field(
                "stats",
                pa.struct(
                    [
                        pa.field("singles_count", pa.int64()),
                        pa.field("singles_total_qty", pa.int64()),
                        pa.field("sealed_count", pa.int64()),
                        pa.field("sealed_total_qty", pa.int64()),
                        pa.field("singles_fetch_s", pa.float64()),
                        pa.field("sealed_fetch_s", pa.float64()),
                        pa.field("sales_fetch_s", pa.float64()),
                        pa.field("sales_concurrency", pa.int64()),
                        pa.field("cards_with_sales_data", pa.int64()),
                        pa.field("total_variants", pa.int64()),
                        pa.field("total_sale_records", pa.int64()),
                        pa.field("batches_total", pa.int64()),
                        pa.field("batches_failed", pa.int64()),
                    ]
                ),
            ),
            pa.field("raw_json", pa.string()),
        ]
    )


def prices_singles_schema() -> pa.Schema:
    """Schema for bronze singles price rows."""
    return pa.schema(
        [
            pa.field("_snapshot_date", pa.string()),
            pa.field("_snapshot_ts_partition", pa.string()),
            pa.field("_snapshot_prefix", pa.string()),
            pa.field("_source_key", pa.string()),
            pa.field("_ingested_at", pa.string()),
            pa.field(
                "meta",
                pa.struct(
                    [
                        pa.field("as_of", pa.string()),
                        pa.field("base_url", pa.string()),
                    ]
                ),
            ),
            pa.field("name", pa.string()),
            pa.field("set_code", pa.string()),
            pa.field("number", pa.string()),
            pa.field("multiverse_id", pa.string()),
            pa.field("scryfall_id", pa.string()),
            pa.field("available_quantity", pa.int64()),
            pa.field("price_cents", pa.int64()),
            pa.field("price_cents_lp_plus", pa.int64()),
            pa.field("price_cents_nm", pa.int64()),
            pa.field("price_cents_foil", pa.int64()),
            pa.field("price_cents_lp_plus_foil", pa.int64()),
            pa.field("price_cents_nm_foil", pa.int64()),
            pa.field("price_cents_etched", pa.int64()),
            pa.field("price_cents_lp_plus_etched", pa.int64()),
            pa.field("price_cents_nm_etched", pa.int64()),
            pa.field("price_market", pa.int64()),
            pa.field("price_market_foil", pa.int64()),
            pa.field("url", pa.string()),
            pa.field("raw_json", pa.string()),
        ]
    )


def prices_sealed_schema() -> pa.Schema:
    """Schema for bronze sealed price rows."""
    return pa.schema(
        [
            pa.field("_snapshot_date", pa.string()),
            pa.field("_snapshot_ts_partition", pa.string()),
            pa.field("_snapshot_prefix", pa.string()),
            pa.field("_source_key", pa.string()),
            pa.field("_ingested_at", pa.string()),
            pa.field(
                "meta",
                pa.struct(
                    [
                        pa.field("as_of", pa.string()),
                        pa.field("base_url", pa.string()),
                    ]
                ),
            ),
            pa.field("product_type", pa.string()),
            pa.field("product_id", pa.string()),
            pa.field("set_code", pa.string()),
            pa.field("name", pa.string()),
            pa.field("tcgplayer_product_id", pa.int64()),
            pa.field("language_id", pa.string()),
            pa.field("low_price", pa.int64()),
            pa.field("available_quantity", pa.int64()),
            pa.field("price_market", pa.int64()),
            pa.field("url", pa.string()),
            pa.field("raw_json", pa.string()),
        ]
    )


def products_cards_schema() -> pa.Schema:
    """Schema for bronze raw product card rows."""
    return pa.schema(
        [
            pa.field("_snapshot_date", pa.string()),
            pa.field("_snapshot_ts_partition", pa.string()),
            pa.field("_snapshot_prefix", pa.string()),
            pa.field("_source_key", pa.string()),
            pa.field("_ingested_at", pa.string()),
            pa.field(
                "meta",
                pa.struct(
                    [
                        pa.field("snapshot_ts", pa.string()),
                        pa.field("cards_requested", pa.int64()),
                        pa.field("cards_returned", pa.int64()),
                        pa.field("batches_total", pa.int64()),
                        pa.field("batches_failed", pa.int64()),
                        pa.field("concurrency", pa.int64()),
                        pa.field("fetch_duration_s", pa.float64()),
                    ]
                ),
            ),
            pa.field("url", pa.string()),
            pa.field("name", pa.string()),
            pa.field("set_code", pa.string()),
            pa.field("number", pa.string()),
            pa.field("multiverse_id", pa.string()),
            pa.field("scryfall_id", pa.string()),
            pa.field("tcgplayer_product_id", pa.int64()),
            pa.field("available_quantity", pa.int64()),
            pa.field("price_cents", pa.int64()),
            pa.field("price_cents_lp_plus", pa.int64()),
            pa.field("price_cents_nm", pa.int64()),
            pa.field("price_cents_foil", pa.int64()),
            pa.field("price_cents_lp_plus_foil", pa.int64()),
            pa.field("price_cents_nm_foil", pa.int64()),
            pa.field("price_cents_etched", pa.int64()),
            pa.field("price_cents_lp_plus_etched", pa.int64()),
            pa.field("price_cents_nm_etched", pa.int64()),
            pa.field("price_market", pa.int64()),
            pa.field("price_market_foil", pa.int64()),
            pa.field("variants", _variant_type()),
            pa.field("raw_json", pa.string()),
        ]
    )


def products_variants_schema() -> pa.Schema:
    """Schema for silver exploded variant rows."""
    return pa.schema(
        [
            pa.field("_snapshot_date", pa.string()),
            pa.field("_snapshot_ts_partition", pa.string()),
            pa.field("_snapshot_prefix", pa.string()),
            pa.field("_source_key", pa.string()),
            pa.field("_ingested_at", pa.string()),
            pa.field("card_url", pa.string()),
            pa.field("card_name", pa.string()),
            pa.field("set_code", pa.string()),
            pa.field("number", pa.string()),
            pa.field("multiverse_id", pa.string()),
            pa.field("scryfall_id", pa.string()),
            pa.field("tcgplayer_product_id", pa.int64()),
            pa.field("card_available_quantity", pa.int64()),
            pa.field("card_price_cents", pa.int64()),
            pa.field("card_price_cents_lp_plus", pa.int64()),
            pa.field("card_price_cents_nm", pa.int64()),
            pa.field("card_price_cents_foil", pa.int64()),
            pa.field("card_price_cents_lp_plus_foil", pa.int64()),
            pa.field("card_price_cents_nm_foil", pa.int64()),
            pa.field("card_price_cents_etched", pa.int64()),
            pa.field("card_price_cents_lp_plus_etched", pa.int64()),
            pa.field("card_price_cents_nm_etched", pa.int64()),
            pa.field("card_price_market", pa.int64()),
            pa.field("card_price_market_foil", pa.int64()),
            pa.field("variant_product_type", pa.string()),
            pa.field("variant_product_id", pa.string()),
            pa.field("variant_tcgplayer_sku_id", pa.int64()),
            pa.field("variant_language_id", pa.string()),
            pa.field("variant_condition_id", pa.string()),
            pa.field("variant_finish_id", pa.string()),
            pa.field("variant_low_price", pa.int64()),
            pa.field("variant_available_quantity", pa.int64()),
            pa.field("recent_sales", _recent_sale_type()),
            pa.field("recent_sales_count", pa.int64()),
            pa.field("variant_raw_json", pa.string()),
        ]
    )


def sales_events_schema() -> pa.Schema:
    """Schema for canonical deduped sales event rows."""
    return pa.schema(
        [
            pa.field("_sale_id", pa.string()),
            pa.field("_sale_date", pa.string()),
            pa.field("_first_seen_snapshot_ts", pa.string()),
            pa.field("_first_seen_snapshot_prefix", pa.string()),
            pa.field("_snapshot_date", pa.string()),
            pa.field("_snapshot_ts_partition", pa.string()),
            pa.field("_snapshot_prefix", pa.string()),
            pa.field("_source_key", pa.string()),
            pa.field("_ingested_at", pa.string()),
            pa.field("card_url", pa.string()),
            pa.field("card_name", pa.string()),
            pa.field("set_code", pa.string()),
            pa.field("number", pa.string()),
            pa.field("multiverse_id", pa.string()),
            pa.field("scryfall_id", pa.string()),
            pa.field("tcgplayer_product_id", pa.int64()),
            pa.field("card_available_quantity", pa.int64()),
            pa.field("card_price_cents", pa.int64()),
            pa.field("card_price_cents_lp_plus", pa.int64()),
            pa.field("card_price_cents_nm", pa.int64()),
            pa.field("card_price_cents_foil", pa.int64()),
            pa.field("card_price_cents_lp_plus_foil", pa.int64()),
            pa.field("card_price_cents_nm_foil", pa.int64()),
            pa.field("card_price_cents_etched", pa.int64()),
            pa.field("card_price_cents_lp_plus_etched", pa.int64()),
            pa.field("card_price_cents_nm_etched", pa.int64()),
            pa.field("card_price_market", pa.int64()),
            pa.field("card_price_market_foil", pa.int64()),
            pa.field("variant_product_type", pa.string()),
            pa.field("variant_product_id", pa.string()),
            pa.field("variant_tcgplayer_sku_id", pa.int64()),
            pa.field("variant_language_id", pa.string()),
            pa.field("variant_condition_id", pa.string()),
            pa.field("variant_finish_id", pa.string()),
            pa.field("variant_low_price", pa.int64()),
            pa.field("variant_available_quantity", pa.int64()),
            pa.field("sale_created_at", pa.string()),
            pa.field("sale_price", pa.int64()),
            pa.field("sale_quantity", pa.int64()),
            pa.field("sale_raw_json", pa.string()),
        ]
    )


def table_schema(table_name: str) -> pa.Schema:
    """Return the Arrow schema for a named bronze or silver table."""
    schemas = {
        "manifests": manifest_schema(),
        "prices_singles_rows": prices_singles_schema(),
        "prices_sealed_rows": prices_sealed_schema(),
        "products_singles_cards_raw": products_cards_schema(),
        "products_singles_variants": products_variants_schema(),
        "sales_events": sales_events_schema(),
    }
    return schemas[table_name]


def build_manifest_rows(
    manifest: dict[str, Any],
    source_key: str,
    *,
    ingested_at: str | None = None,
) -> list[dict[str, Any]]:
    """Mirror manifest JSON into one bronze row."""
    row = {
        **_snapshot_provenance(manifest, source_key, ingested_at=ingested_at),
        "snapshot_ts": manifest.get("snapshot_ts"),
        "prefix": manifest.get("prefix"),
        "files": {
            "prices_singles": manifest.get("files", {}).get("prices_singles"),
            "prices_sealed": manifest.get("files", {}).get("prices_sealed"),
            "products_singles": manifest.get("files", {}).get("products_singles"),
        },
        "stats": {
            "singles_count": manifest.get("stats", {}).get("singles_count"),
            "singles_total_qty": manifest.get("stats", {}).get("singles_total_qty"),
            "sealed_count": manifest.get("stats", {}).get("sealed_count"),
            "sealed_total_qty": manifest.get("stats", {}).get("sealed_total_qty"),
            "singles_fetch_s": manifest.get("stats", {}).get("singles_fetch_s"),
            "sealed_fetch_s": manifest.get("stats", {}).get("sealed_fetch_s"),
            "sales_fetch_s": manifest.get("stats", {}).get("sales_fetch_s"),
            "sales_concurrency": manifest.get("stats", {}).get("sales_concurrency"),
            "cards_with_sales_data": manifest.get("stats", {}).get("cards_with_sales_data"),
            "total_variants": manifest.get("stats", {}).get("total_variants"),
            "total_sale_records": manifest.get("stats", {}).get("total_sale_records"),
            "batches_total": manifest.get("stats", {}).get("batches_total"),
            "batches_failed": manifest.get("stats", {}).get("batches_failed"),
        },
        "raw_json": _json_string(manifest),
    }
    return [row]


def build_prices_singles_rows(
    payload: dict[str, Any],
    manifest: dict[str, Any],
    source_key: str,
    *,
    ingested_at: str | None = None,
) -> list[dict[str, Any]]:
    """Mirror `prices_singles.data[]` rows into bronze."""
    meta = payload.get("meta", {})
    provenance = _snapshot_provenance(manifest, source_key, ingested_at=ingested_at)
    rows = []
    for item in payload.get("data", []):
        rows.append(
            {
                **provenance,
                "meta": {"as_of": meta.get("as_of"), "base_url": meta.get("base_url")},
                "name": item.get("name"),
                "set_code": item.get("set_code"),
                "number": item.get("number"),
                "multiverse_id": item.get("multiverse_id"),
                "scryfall_id": item.get("scryfall_id"),
                "available_quantity": item.get("available_quantity"),
                "price_cents": item.get("price_cents"),
                "price_cents_lp_plus": item.get("price_cents_lp_plus"),
                "price_cents_nm": item.get("price_cents_nm"),
                "price_cents_foil": item.get("price_cents_foil"),
                "price_cents_lp_plus_foil": item.get("price_cents_lp_plus_foil"),
                "price_cents_nm_foil": item.get("price_cents_nm_foil"),
                "price_cents_etched": item.get("price_cents_etched"),
                "price_cents_lp_plus_etched": item.get("price_cents_lp_plus_etched"),
                "price_cents_nm_etched": item.get("price_cents_nm_etched"),
                "price_market": item.get("price_market"),
                "price_market_foil": item.get("price_market_foil"),
                "url": item.get("url"),
                "raw_json": _json_string(item),
            }
        )
    return rows


def build_prices_sealed_rows(
    payload: dict[str, Any],
    manifest: dict[str, Any],
    source_key: str,
    *,
    ingested_at: str | None = None,
) -> list[dict[str, Any]]:
    """Mirror `prices_sealed.data[]` rows into bronze."""
    meta = payload.get("meta", {})
    provenance = _snapshot_provenance(manifest, source_key, ingested_at=ingested_at)
    rows = []
    for item in payload.get("data", []):
        rows.append(
            {
                **provenance,
                "meta": {"as_of": meta.get("as_of"), "base_url": meta.get("base_url")},
                "product_type": item.get("product_type"),
                "product_id": item.get("product_id"),
                "set_code": item.get("set_code"),
                "name": item.get("name"),
                "tcgplayer_product_id": item.get("tcgplayer_product_id"),
                "language_id": item.get("language_id"),
                "low_price": item.get("low_price"),
                "available_quantity": item.get("available_quantity"),
                "price_market": item.get("price_market"),
                "url": item.get("url"),
                "raw_json": _json_string(item),
            }
        )
    return rows


def build_products_cards_rows(
    payload: dict[str, Any],
    manifest: dict[str, Any],
    source_key: str,
    *,
    ingested_at: str | None = None,
) -> list[dict[str, Any]]:
    """Mirror `products_singles.data[]` rows into bronze with nested variants intact."""
    meta = payload.get("meta", {})
    provenance = _snapshot_provenance(manifest, source_key, ingested_at=ingested_at)
    rows = []
    for item in payload.get("data", []):
        rows.append(
            {
                **provenance,
                "meta": {
                    "snapshot_ts": meta.get("snapshot_ts"),
                    "cards_requested": meta.get("cards_requested"),
                    "cards_returned": meta.get("cards_returned"),
                    "batches_total": meta.get("batches_total"),
                    "batches_failed": meta.get("batches_failed"),
                    "concurrency": meta.get("concurrency"),
                    "fetch_duration_s": meta.get("fetch_duration_s"),
                },
                "url": item.get("url"),
                "name": item.get("name"),
                "set_code": item.get("set_code"),
                "number": item.get("number"),
                "multiverse_id": item.get("multiverse_id"),
                "scryfall_id": item.get("scryfall_id"),
                "tcgplayer_product_id": item.get("tcgplayer_product_id"),
                "available_quantity": item.get("available_quantity"),
                "price_cents": item.get("price_cents"),
                "price_cents_lp_plus": item.get("price_cents_lp_plus"),
                "price_cents_nm": item.get("price_cents_nm"),
                "price_cents_foil": item.get("price_cents_foil"),
                "price_cents_lp_plus_foil": item.get("price_cents_lp_plus_foil"),
                "price_cents_nm_foil": item.get("price_cents_nm_foil"),
                "price_cents_etched": item.get("price_cents_etched"),
                "price_cents_lp_plus_etched": item.get("price_cents_lp_plus_etched"),
                "price_cents_nm_etched": item.get("price_cents_nm_etched"),
                "price_market": item.get("price_market"),
                "price_market_foil": item.get("price_market_foil"),
                "variants": item.get("variants", []),
                "raw_json": _json_string(item),
            }
        )
    return rows


def build_products_variant_rows(
    payload: dict[str, Any],
    manifest: dict[str, Any],
    source_key: str,
    *,
    ingested_at: str | None = None,
) -> list[dict[str, Any]]:
    """Explode product variants into queryable silver rows."""
    provenance = _snapshot_provenance(manifest, source_key, ingested_at=ingested_at)
    rows = []
    for item in payload.get("data", []):
        card_context = {
            "card_url": item.get("url"),
            "card_name": item.get("name"),
            "set_code": item.get("set_code"),
            "number": item.get("number"),
            "multiverse_id": item.get("multiverse_id"),
            "scryfall_id": item.get("scryfall_id"),
            "tcgplayer_product_id": item.get("tcgplayer_product_id"),
            "card_available_quantity": item.get("available_quantity"),
            "card_price_cents": item.get("price_cents"),
            "card_price_cents_lp_plus": item.get("price_cents_lp_plus"),
            "card_price_cents_nm": item.get("price_cents_nm"),
            "card_price_cents_foil": item.get("price_cents_foil"),
            "card_price_cents_lp_plus_foil": item.get("price_cents_lp_plus_foil"),
            "card_price_cents_nm_foil": item.get("price_cents_nm_foil"),
            "card_price_cents_etched": item.get("price_cents_etched"),
            "card_price_cents_lp_plus_etched": item.get("price_cents_lp_plus_etched"),
            "card_price_cents_nm_etched": item.get("price_cents_nm_etched"),
            "card_price_market": item.get("price_market"),
            "card_price_market_foil": item.get("price_market_foil"),
        }
        for variant in item.get("variants", []):
            rows.append(
                {
                    **provenance,
                    **card_context,
                    "variant_product_type": variant.get("product_type"),
                    "variant_product_id": variant.get("product_id"),
                    "variant_tcgplayer_sku_id": variant.get("tcgplayer_sku_id"),
                    "variant_language_id": variant.get("language_id"),
                    "variant_condition_id": variant.get("condition_id"),
                    "variant_finish_id": variant.get("finish_id"),
                    "variant_low_price": variant.get("low_price"),
                    "variant_available_quantity": variant.get("available_quantity"),
                    "recent_sales": variant.get("recent_sales", []),
                    "recent_sales_count": len(variant.get("recent_sales", [])),
                    "variant_raw_json": _json_string(variant),
                }
            )
    return rows


def build_sales_event_rows(
    payload: dict[str, Any],
    manifest: dict[str, Any],
    source_key: str,
    *,
    ingested_at: str | None = None,
) -> list[dict[str, Any]]:
    """Explode product sales into canonical silver event rows."""
    provenance = _snapshot_provenance(manifest, source_key, ingested_at=ingested_at)
    rows = []
    skipped_missing_created_at = 0
    for item in payload.get("data", []):
        card_context = {
            "card_url": item.get("url"),
            "card_name": item.get("name"),
            "set_code": item.get("set_code"),
            "number": item.get("number"),
            "multiverse_id": item.get("multiverse_id"),
            "scryfall_id": item.get("scryfall_id"),
            "tcgplayer_product_id": item.get("tcgplayer_product_id"),
            "card_available_quantity": item.get("available_quantity"),
            "card_price_cents": item.get("price_cents"),
            "card_price_cents_lp_plus": item.get("price_cents_lp_plus"),
            "card_price_cents_nm": item.get("price_cents_nm"),
            "card_price_cents_foil": item.get("price_cents_foil"),
            "card_price_cents_lp_plus_foil": item.get("price_cents_lp_plus_foil"),
            "card_price_cents_nm_foil": item.get("price_cents_nm_foil"),
            "card_price_cents_etched": item.get("price_cents_etched"),
            "card_price_cents_lp_plus_etched": item.get("price_cents_lp_plus_etched"),
            "card_price_cents_nm_etched": item.get("price_cents_nm_etched"),
            "card_price_market": item.get("price_market"),
            "card_price_market_foil": item.get("price_market_foil"),
        }
        for variant in item.get("variants", []):
            variant_context = {
                "variant_product_type": variant.get("product_type"),
                "variant_product_id": variant.get("product_id"),
                "variant_tcgplayer_sku_id": variant.get("tcgplayer_sku_id"),
                "variant_language_id": variant.get("language_id"),
                "variant_condition_id": variant.get("condition_id"),
                "variant_finish_id": variant.get("finish_id"),
                "variant_low_price": variant.get("low_price"),
                "variant_available_quantity": variant.get("available_quantity"),
            }
            for sale in variant.get("recent_sales", []):
                sale_created_at = sale.get("created_at")
                sale_date = _sale_date(sale_created_at)
                if not sale_date:
                    # Skip malformed sales rows so one bad record does not abort the entire snapshot write.
                    skipped_missing_created_at += 1
                    continue
                rows.append(
                    {
                        "_sale_id": _sale_identity(variant, sale),
                        "_sale_date": sale_date,
                        "_first_seen_snapshot_ts": manifest.get("snapshot_ts"),
                        "_first_seen_snapshot_prefix": manifest.get("prefix"),
                        **provenance,
                        **card_context,
                        **variant_context,
                        "sale_created_at": sale_created_at,
                        "sale_price": sale.get("price"),
                        "sale_quantity": sale.get("quantity"),
                        "sale_raw_json": _json_string(sale),
                    }
                )
    if skipped_missing_created_at:
        _log_workflow(
            f"Skipped {skipped_missing_created_at:,} sale rows without a valid created_at timestamp"
        )
    return rows


def _rows_to_table(rows: list[dict[str, Any]], schema: pa.Schema) -> pa.Table:
    """Create an Arrow table while preserving the expected schema for empty writes."""
    return pa.Table.from_pylist(rows, schema=schema)


def _write_parquet_bytes(rows: list[dict[str, Any]], schema: pa.Schema) -> bytes:
    """Serialize rows into a Parquet object in memory."""
    table = _rows_to_table(rows, schema)
    sink = BytesIO()
    pq.write_table(table, sink, compression="snappy")
    return sink.getvalue()


def put_parquet_rows(
    client,
    bucket: str,
    key: str,
    rows: list[dict[str, Any]],
    schema: pa.Schema,
) -> None:
    """Write a list of rows to a single Parquet object in R2."""
    _log_workflow(f"Writing Parquet object {key} ({len(rows):,} rows)")
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=_write_parquet_bytes(rows, schema),
        ContentType=PARQUET_CONTENT_TYPE,
    )


def parquet_snapshot_key(layer: str, table_name: str, manifest: dict[str, Any]) -> str:
    """Build the canonical snapshot-partitioned Parquet object key."""
    snapshot_ts_partition = _normalize_snapshot_partition(manifest["snapshot_ts"])
    snapshot_date = manifest["prefix"].split("/")[1]
    return (
        f"{PARQUET_ROOT}/{layer}/{table_name}/snapshot_date={snapshot_date}/"
        f"snapshot_ts={snapshot_ts_partition}/part-00000.parquet"
    )


def parquet_sales_key(sale_date: str, manifest: dict[str, Any]) -> str:
    """Build the canonical sales-events Parquet object key for one snapshot run."""
    snapshot_ts_partition = _normalize_snapshot_partition(manifest["snapshot_ts"])
    return f"{sales_partition_prefix(sale_date)}part-{snapshot_ts_partition}.parquet"


def read_parquet_table(client, bucket: str, key: str, columns: list[str] | None = None) -> pa.Table:
    """Read a Parquet table from R2."""
    _log_workflow(f"Reading Parquet object {key}")
    obj = client.get_object(Bucket=bucket, Key=key)
    return pq.read_table(BytesIO(obj["Body"].read()), columns=columns)


def read_parquet_rows(
    client,
    bucket: str,
    prefix: str,
    *,
    columns: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Read all Parquet rows under a prefix into Python dicts."""
    keys = [key for key in list_object_keys(client, bucket, prefix) if key.endswith(".parquet")]
    rows: list[dict[str, Any]] = []
    for key in sorted(keys):
        table = read_parquet_table(client, bucket, key, columns=columns)
        rows.extend(table.to_pylist())
    return rows


def _require_duckdb():
    """Import DuckDB lazily so non-DuckDB workflows still import cleanly."""
    try:
        import duckdb  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "DuckDB is required for efficient Parquet sales scans. Install it with `pip install duckdb`."
        ) from exc
    return duckdb


def _configure_duckdb_s3(connection) -> None:
    """Configure DuckDB to read Parquet directly from Cloudflare R2."""
    global _DUCKDB_HTTPFS_INSTALLED
    account_id = os.environ["R2_ACCOUNT_ID"]
    try:
        connection.execute("LOAD httpfs")
    except Exception:
        # Install the extension once per process instead of paying the network cost on every query.
        if not _DUCKDB_HTTPFS_INSTALLED:
            connection.execute("INSTALL httpfs")
            _DUCKDB_HTTPFS_INSTALLED = True
        connection.execute("LOAD httpfs")
    connection.execute("SET s3_region='auto'")
    connection.execute("SET s3_url_style='path'")
    connection.execute("SET s3_use_ssl=true")
    _duckdb_set_string(connection, "s3_endpoint", f"{account_id}.r2.cloudflarestorage.com")
    _duckdb_set_string(connection, "s3_access_key_id", os.environ["R2_ACCESS_KEY_ID"])
    _duckdb_set_string(connection, "s3_secret_access_key", os.environ["R2_SECRET_ACCESS_KEY"])


def _s3_url(bucket: str, key: str) -> str:
    """Convert an object key into the S3 URL DuckDB expects."""
    return f"s3://{bucket}/{key}"


def _sql_quote(value: str) -> str:
    """Safely quote string literals embedded into DuckDB SQL."""
    return "'" + value.replace("'", "''") + "'"


def _duckdb_set_string(connection, setting: str, value: str) -> None:
    """Quote DuckDB string settings so credentials do not break SQL parsing."""
    connection.execute(f"SET {setting}={_sql_quote(value)}")


def duckdb_query_parquet(
    client,
    bucket: str,
    prefixes: list[str],
    *,
    select_sql: str,
    where_clauses: list[str] | None = None,
    group_by_sql: str | None = None,
    order_by_sql: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Query Parquet files under one or more prefixes using DuckDB instead of sequential reads."""
    keys: list[str] = []
    for prefix in prefixes:
        keys.extend(key for key in list_object_keys(client, bucket, prefix) if key.endswith(".parquet"))

    if not keys:
        return []

    duckdb = _require_duckdb()
    connection = duckdb.connect(database=":memory:")
    _configure_duckdb_s3(connection)

    file_list = ", ".join(_sql_quote(_s3_url(bucket, key)) for key in sorted(keys))
    sql = f"{select_sql} FROM read_parquet([{file_list}], hive_partitioning=true)"
    if where_clauses:
        sql += " WHERE " + " AND ".join(where_clauses)
    if group_by_sql:
        sql += f" GROUP BY {group_by_sql}"
    if order_by_sql:
        sql += f" ORDER BY {order_by_sql}"
    if limit is not None:
        sql += f" LIMIT {limit}"

    _log_workflow(f"Running DuckDB parquet query across {len(keys):,} file(s)")
    return connection.execute(sql).fetch_arrow_table().to_pylist()


def load_existing_sale_ids(
    client,
    bucket: str,
    *,
    sale_dates: set[str] | None = None,
) -> set[str]:
    """Load canonical sale ids from the existing sales_events partitions."""
    if sale_dates:
        prefixes = [sales_partition_prefix(sale_date) for sale_date in sorted(sale_dates)]
    else:
        prefixes = [f"{PARQUET_ROOT}/silver/sales_events/"]

    sale_ids = {
        row["_sale_id"]
        for row in duckdb_query_parquet(
            client,
            bucket,
            prefixes,
            select_sql="SELECT DISTINCT _sale_id",
        )
        if row.get("_sale_id")
    }

    _log_workflow(f"Loaded {len(sale_ids):,} existing sale ids")
    return sale_ids


def write_snapshot_parquet(
    client,
    bucket: str,
    manifest: dict[str, Any],
    *,
    manifest_payload: dict[str, Any],
    prices_singles_payload: dict[str, Any],
    prices_sealed_payload: dict[str, Any] | None,
    products_singles_payload: dict[str, Any] | None,
    existing_sale_ids: set[str] | None = None,
    ingested_at: str | None = None,
) -> dict[str, int]:
    """Write all bronze and silver Parquet objects derived from one raw snapshot."""
    source_keys = get_snapshot_source_keys(manifest)
    counts: dict[str, int] = {}

    manifest_rows = build_manifest_rows(
        manifest_payload, source_keys["manifest"], ingested_at=ingested_at
    )
    put_parquet_rows(
        client,
        bucket,
        parquet_snapshot_key("bronze", "manifests", manifest),
        manifest_rows,
        manifest_schema(),
    )
    counts["manifests"] = len(manifest_rows)

    singles_rows = build_prices_singles_rows(
        prices_singles_payload,
        manifest,
        source_keys["prices_singles"],
        ingested_at=ingested_at,
    )
    put_parquet_rows(
        client,
        bucket,
        parquet_snapshot_key("bronze", "prices_singles_rows", manifest),
        singles_rows,
        prices_singles_schema(),
    )
    counts["prices_singles_rows"] = len(singles_rows)

    if prices_sealed_payload is not None:
        sealed_rows = build_prices_sealed_rows(
            prices_sealed_payload,
            manifest,
            source_keys["prices_sealed"],
            ingested_at=ingested_at,
        )
        put_parquet_rows(
            client,
            bucket,
            parquet_snapshot_key("bronze", "prices_sealed_rows", manifest),
            sealed_rows,
            prices_sealed_schema(),
        )
        counts["prices_sealed_rows"] = len(sealed_rows)
    else:
        counts["prices_sealed_rows"] = 0

    if products_singles_payload is None:
        counts["products_singles_cards_raw"] = 0
        counts["products_singles_variants"] = 0
        counts["sales_events"] = 0
        return counts

    cards_rows = build_products_cards_rows(
        products_singles_payload,
        manifest,
        source_keys["products_singles"],
        ingested_at=ingested_at,
    )
    put_parquet_rows(
        client,
        bucket,
        parquet_snapshot_key("bronze", "products_singles_cards_raw", manifest),
        cards_rows,
        products_cards_schema(),
    )
    counts["products_singles_cards_raw"] = len(cards_rows)

    variant_rows = build_products_variant_rows(
        products_singles_payload,
        manifest,
        source_keys["products_singles"],
        ingested_at=ingested_at,
    )
    put_parquet_rows(
        client,
        bucket,
        parquet_snapshot_key("silver", "products_singles_variants", manifest),
        variant_rows,
        products_variants_schema(),
    )
    counts["products_singles_variants"] = len(variant_rows)

    sales_rows = build_sales_event_rows(
        products_singles_payload,
        manifest,
        source_keys["products_singles"],
        ingested_at=ingested_at,
    )
    sale_dates = {row["_sale_date"] for row in sales_rows if row.get("_sale_date")}
    if existing_sale_ids is None:
        existing_sale_ids = load_existing_sale_ids(client, bucket, sale_dates=sale_dates)

    deduped_rows = []
    seen_sale_ids = set(existing_sale_ids)
    for row in sales_rows:
        sale_id = row["_sale_id"]
        if sale_id in seen_sale_ids:
            continue
        deduped_rows.append(row)
        seen_sale_ids.add(sale_id)

    existing_sale_ids.update(seen_sale_ids)

    by_sale_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in deduped_rows:
        by_sale_date[row["_sale_date"]].append(row)

    for sale_date, rows in by_sale_date.items():
        put_parquet_rows(
            client,
            bucket,
            parquet_sales_key(sale_date, manifest),
            rows,
            sales_events_schema(),
        )

    counts["sales_events"] = len(deduped_rows)
    _log_workflow(
        "Completed Parquet write for snapshot "
        f"{manifest['prefix']} with {counts['sales_events']:,} canonical sales rows"
    )
    return counts
