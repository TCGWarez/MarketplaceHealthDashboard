#!/usr/bin/env python3
"""Mana Pool R2 data agent built around explicit raw and Parquet query helpers."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

import boto3
from botocore.config import Config
from dotenv import load_dotenv

from mp_parquet import (
    PARQUET_ROOT,
    duckdb_query_parquet,
    list_snapshot_manifests,
    read_json_object,
    read_parquet_rows,
    sales_partition_prefix,
)


load_dotenv()

FILE_TAG = "mp_r2_agent.py"
BUCKET = os.environ.get("R2_BUCKET", "manapool-snapshots")
SNAPSHOT_FILES = {
    "manifest": "manifest.json.gz",
    "prices_singles": "prices_singles.json.gz",
    "prices_sealed": "prices_sealed.json.gz",
    "products_singles": "products_singles.json.gz",
}
SNAPSHOT_TABLE_LAYERS = {
    "manifests": "bronze",
    "prices_singles_rows": "bronze",
    "prices_sealed_rows": "bronze",
    "products_singles_cards_raw": "bronze",
    "products_singles_variants": "silver",
}
SALES_GROUP_ALIASES = {
    "card": "card",
    "cards": "card",
    "product": "variant",
    "products": "variant",
    "sku": "variant",
    "skus": "variant",
    "variant": "variant",
    "variants": "variant",
}
SALES_SORT_ALIASES = {
    "count": "sales_count",
    "gross": "gross_cents",
    "gross_cents": "gross_cents",
    "latest": "latest_sale_at",
    "latest_sale_at": "latest_sale_at",
    "revenue": "gross_cents",
    "sales": "sales_count",
    "sales_count": "sales_count",
    "units": "units_sold",
    "units_sold": "units_sold",
    "volume": "units_sold",
}


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("mana_pool_r2_agent")


def _log_workflow(message: str) -> None:
    """Keep logs attributable to this module during query runs."""
    log.info(f"{FILE_TAG}: {message}")


def get_r2_client():
    """Create an R2 client that can read both raw JSON and Parquet objects."""
    _log_workflow("Creating R2 client")
    account_id = os.environ["R2_ACCOUNT_ID"]
    return boto3.client(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
        config=Config(response_checksum_validation="when_required"),
    )


def _parse_snapshot_ts(value: str) -> datetime:
    """Convert ISO timestamps into timezone-aware datetimes."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _normalize_snapshot_partition(snapshot_ts: str) -> str:
    """Convert ISO timestamps to the partition format used in Parquet paths."""
    parsed = datetime.fromisoformat(snapshot_ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    return parsed.strftime("%Y%m%dT%H%M%SZ")


def _sql_quote(value: str) -> str:
    """Safely quote string literals embedded into DuckDB SQL."""
    return "'" + value.replace("'", "''") + "'"


def _sql_like(value: str) -> str:
    """Quote LIKE patterns while escaping literal `%` and `_` characters."""
    escaped = value.replace("!", "!!").replace("%", "!%").replace("_", "!_").replace("'", "''")
    return f"'%{escaped}%' ESCAPE '!'"


def get_latest_snapshot(client, bucket: str = BUCKET) -> dict[str, Any]:
    """Read the latest raw snapshot manifest pointer."""
    _log_workflow("Loading latest snapshot manifest")
    return read_json_object(client, bucket, "latest.json.gz")


def list_snapshots(client, limit: int = 10, bucket: str = BUCKET) -> list[dict[str, Any]]:
    """List raw snapshot manifests sorted newest-first."""
    manifests = list_snapshot_manifests(client, bucket)
    manifests.sort(key=lambda item: item["snapshot_ts"], reverse=True)
    return manifests[:limit]


def _resolve_snapshot_key(prefix: str, dataset: str) -> str:
    """Resolve a raw snapshot dataset name to an object key."""
    filename = SNAPSHOT_FILES.get(dataset, dataset)
    if filename.startswith(prefix):
        return filename
    return f"{prefix}/{filename}"


def load_snapshot_data(
    client,
    prefix: str,
    dataset: str,
    bucket: str = BUCKET,
) -> dict[str, Any]:
    """Load a raw snapshot dataset from R2."""
    key = _resolve_snapshot_key(prefix, dataset)
    _log_workflow(f"Loading raw dataset {key}")
    return read_json_object(client, bucket, key)


def load_latest_dataset(client, dataset: str, bucket: str = BUCKET) -> dict[str, Any]:
    """Resolve the latest manifest and load one raw dataset from that snapshot."""
    latest = get_latest_snapshot(client, bucket=bucket)
    return load_snapshot_data(client, latest["prefix"], dataset, bucket=bucket)


def describe_dataset(data: dict[str, Any]) -> dict[str, Any]:
    """Return a compact structural summary for one raw `meta` + `data` payload."""
    items = data.get("data", [])
    first_item = items[0] if items else {}
    return {
        "top_level_keys": list(data.keys()),
        "item_count": len(items),
        "meta_keys": list(data.get("meta", {}).keys()),
        "first_item_keys": list(first_item.keys()) if isinstance(first_item, dict) else [],
    }


def describe_snapshot_schema(
    client,
    prefix: str | None = None,
    bucket: str = BUCKET,
) -> dict[str, Any]:
    """Describe the raw snapshot shape for the latest or selected snapshot."""
    manifest = get_latest_snapshot(client, bucket=bucket) if prefix is None else load_snapshot_data(client, prefix, "manifest", bucket=bucket)
    prefix = manifest["prefix"]
    files = manifest.get("files", {})
    prices_singles = load_snapshot_data(client, prefix, "prices_singles", bucket=bucket)
    prices_sealed = (
        load_snapshot_data(client, prefix, "prices_sealed", bucket=bucket)
        if files.get("prices_sealed")
        else {"meta": {}, "data": []}
    )
    products_singles = (
        load_snapshot_data(client, prefix, "products_singles", bucket=bucket)
        if files.get("products_singles")
        else {"meta": {}, "data": []}
    )

    first_product = products_singles.get("data", [None])[0] or {}
    first_variant = first_product.get("variants", [None])[0] or {}
    first_sale = first_variant.get("recent_sales", [None])[0] or {}

    return {
        "manifest": manifest,
        "prices_singles": describe_dataset(prices_singles),
        "prices_sealed": describe_dataset(prices_sealed),
        "products_singles": {
            "top_level_keys": list(products_singles.keys()),
            "item_count": len(products_singles.get("data", [])),
            "meta_keys": list(products_singles.get("meta", {}).keys()),
            "first_item_keys": list(first_product.keys()),
            "first_variant_keys": list(first_variant.keys()),
            "first_sale_keys": list(first_sale.keys()),
        },
    }


def iter_cards(products_data: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Yield raw card rows from `products_singles`."""
    yield from products_data.get("data", [])


def iter_variant_rows(products_data: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Flatten raw card + variant data into query-friendly rows."""
    for card in iter_cards(products_data):
        card_context = {
            "card_name": card.get("name"),
            "set_code": card.get("set_code"),
            "number": card.get("number"),
            "multiverse_id": card.get("multiverse_id"),
            "scryfall_id": card.get("scryfall_id"),
            "tcgplayer_product_id": card.get("tcgplayer_product_id"),
            "card_url": card.get("url"),
            "card_available_quantity": card.get("available_quantity"),
            "card_price_cents": card.get("price_cents"),
            "card_price_market": card.get("price_market"),
        }
        for variant in card.get("variants", []):
            yield {
                **card_context,
                "product_type": variant.get("product_type"),
                "product_id": variant.get("product_id"),
                "tcgplayer_sku_id": variant.get("tcgplayer_sku_id"),
                "language_id": variant.get("language_id"),
                "condition_id": variant.get("condition_id"),
                "finish_id": variant.get("finish_id"),
                "variant_low_price": variant.get("low_price"),
                "variant_available_quantity": variant.get("available_quantity"),
                "recent_sales_count": len(variant.get("recent_sales", [])),
            }


def search_cards(data: dict[str, Any], query: str, limit: int = 20) -> list[dict[str, Any]]:
    """Search raw card-like datasets by substring on card name."""
    query_lower = query.strip().lower()
    results = []
    for card in data.get("data", []):
        if query_lower in (card.get("name") or "").lower():
            results.append(card)
        if len(results) >= limit:
            break
    _log_workflow(f"Search for '{query}' returned {len(results)} raw rows")
    return results


def filter_variant_rows(
    variant_rows: list[dict[str, Any]],
    *,
    name_query: str | None = None,
    set_code: str | None = None,
    language_id: str | None = None,
    condition_id: str | None = None,
    finish_id: str | None = None,
    min_price: int | None = None,
    max_price: int | None = None,
    min_quantity: int | None = None,
    require_sales: bool = False,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Filter variant rows from either raw or Parquet data."""
    rows = []
    normalized_name = name_query.lower() if name_query else None
    for row in variant_rows:
        card_name = row.get("card_name") or row.get("card_name")
        if normalized_name and normalized_name not in (card_name or "").lower():
            continue
        if set_code and row.get("set_code") != set_code:
            continue
        if language_id and row.get("variant_language_id", row.get("language_id")) != language_id:
            continue
        if condition_id and row.get("variant_condition_id", row.get("condition_id")) != condition_id:
            continue
        if finish_id and row.get("variant_finish_id", row.get("finish_id")) != finish_id:
            continue
        price_value = row.get("variant_low_price")
        if min_price is not None and (price_value or 0) < min_price:
            continue
        if max_price is not None and (price_value or 0) > max_price:
            continue
        quantity_value = row.get("variant_available_quantity")
        if min_quantity is not None and (quantity_value or 0) < min_quantity:
            continue
        if require_sales and not row.get("recent_sales_count"):
            continue
        rows.append(row)
        if limit is not None and len(rows) >= limit:
            break
    _log_workflow(f"Variant filter returned {len(rows)} rows")
    return rows


def _snapshot_table_prefix(table_name: str, manifest: dict[str, Any]) -> str:
    """Build a snapshot-partitioned Parquet prefix for a bronze or silver table."""
    layer = SNAPSHOT_TABLE_LAYERS[table_name]
    snapshot_date = manifest["prefix"].split("/")[1]
    snapshot_ts_partition = _normalize_snapshot_partition(manifest["snapshot_ts"])
    return (
        f"{PARQUET_ROOT}/{layer}/{table_name}/snapshot_date={snapshot_date}/"
        f"snapshot_ts={snapshot_ts_partition}/"
    )


def load_snapshot_table_rows(
    client,
    table_name: str,
    manifest: dict[str, Any],
    *,
    bucket: str = BUCKET,
) -> list[dict[str, Any]]:
    """Load one snapshot-partitioned bronze or silver Parquet table."""
    prefix = _snapshot_table_prefix(table_name, manifest)
    _log_workflow(f"Loading Parquet rows from {prefix}")
    return read_parquet_rows(client, bucket, prefix)


def load_latest_variant_snapshot(client, bucket: str = BUCKET) -> list[dict[str, Any]]:
    """Load the latest silver variant snapshot partition."""
    latest = get_latest_snapshot(client, bucket=bucket)
    return load_snapshot_table_rows(client, "products_singles_variants", latest, bucket=bucket)


def _date_range(start_date: date, end_date: date) -> list[str]:
    """Generate inclusive partition dates for canonical sales scans."""
    current = start_date
    values = []
    while current <= end_date:
        values.append(current.isoformat())
        current += timedelta(days=1)
    return values


def _coerce_date(value: str | None) -> date | None:
    """Parse `YYYY-MM-DD` date strings used by sales-event partitions."""
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def _normalize_sales_group_by(group_by: str) -> str:
    """Map common grouping aliases to the explicit summary grain."""
    normalized = SALES_GROUP_ALIASES.get((group_by or "").strip().lower())
    if normalized is None:
        raise ValueError(f"Unsupported sales group: {group_by!r}")
    return normalized


def _normalize_sales_sort_by(sort_by: str) -> str:
    """Map common sales ranking aliases to canonical metric keys."""
    normalized = SALES_SORT_ALIASES.get((sort_by or "").strip().lower())
    if normalized is None:
        raise ValueError(f"Unsupported sales sort: {sort_by!r}")
    return normalized


def _coerce_date_value(value: str | date | None) -> date | None:
    """Accept either `date` objects or `YYYY-MM-DD` strings for relative windows."""
    if isinstance(value, date):
        return value
    return _coerce_date(value)


def _resolve_relative_window(
    *,
    days: int,
    end_date: str | date | None = None,
    today: date | None = None,
) -> tuple[str, str]:
    """Turn relative-day windows into inclusive ISO start/end dates."""
    if days < 1:
        raise ValueError("`days` must be at least 1.")
    resolved_end = _coerce_date_value(end_date) or today or date.today()
    resolved_start = resolved_end - timedelta(days=days - 1)
    return resolved_start.isoformat(), resolved_end.isoformat()


def _sales_time_range_payload(
    *,
    sale_date: str | None,
    start_date: str | None,
    end_date: str | None,
) -> dict[str, Any]:
    """Emit a stable time-range description alongside sales query results."""
    if sale_date:
        return {"kind": "single_day", "sale_date": sale_date}
    return {
        "kind": "date_range",
        "start_date": start_date,
        "end_date": end_date,
    }


def scan_sales_events(
    client,
    *,
    sale_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    card_query: str | None = None,
    set_code: str | None = None,
    bucket: str = BUCKET,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Scan canonical sales events through DuckDB for better large-partition performance."""
    if sale_date:
        sale_dates = [sale_date]
    else:
        start = _coerce_date(start_date)
        end = _coerce_date(end_date)
        if start and end:
            sale_dates = _date_range(start, end)
        elif start:
            sale_dates = [start.isoformat()]
        elif end:
            sale_dates = [end.isoformat()]
        else:
            raise ValueError("Provide `sale_date` or `start_date`/`end_date` for canonical sales scans.")

    prefixes = [sales_partition_prefix(current_date) for current_date in sale_dates]
    where_clauses = []
    if card_query:
        where_clauses.append(
            f"lower(card_name) LIKE {_sql_like(card_query.lower())}"
        )
    if set_code:
        where_clauses.append(f"set_code = {_sql_quote(set_code)}")

    rows = duckdb_query_parquet(
        client,
        bucket,
        prefixes,
        select_sql="SELECT *",
        where_clauses=where_clauses,
        order_by_sql="sale_created_at DESC",
        limit=limit,
    )
    _log_workflow(f"Sales scan returned {len(rows)} canonical rows")
    return rows


def summarize_sales_events(
    sale_rows: list[dict[str, Any]],
    *,
    top_n: int = 20,
    group_by: str = "card",
    sort_by: str = "units_sold",
) -> list[dict[str, Any]]:
    """Aggregate canonical sales rows by card or card-variant."""
    normalized_group_by = _normalize_sales_group_by(group_by)
    normalized_sort_by = _normalize_sales_sort_by(sort_by)
    buckets: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in sale_rows:
        if normalized_group_by == "variant":
            key = (
                row.get("card_name"),
                row.get("set_code"),
                row.get("variant_language_id"),
                row.get("variant_condition_id"),
                row.get("variant_finish_id"),
            )
        else:
            key = (row.get("card_name"), row.get("set_code"))

        if key not in buckets:
            buckets[key] = {
                "card_name": row.get("card_name"),
                "set_code": row.get("set_code"),
                "variant_language_id": row.get("variant_language_id") if normalized_group_by == "variant" else None,
                "variant_condition_id": row.get("variant_condition_id") if normalized_group_by == "variant" else None,
                "variant_finish_id": row.get("variant_finish_id") if normalized_group_by == "variant" else None,
                "sales_count": 0,
                "units_sold": 0,
                "gross_cents": 0,
                "latest_sale_at": row.get("sale_created_at"),
            }

        bucket = buckets[key]
        bucket["sales_count"] += 1
        bucket["units_sold"] += row.get("sale_quantity") or 0
        bucket["gross_cents"] += (row.get("sale_price") or 0) * (row.get("sale_quantity") or 0)
        if (row.get("sale_created_at") or "") > (bucket.get("latest_sale_at") or ""):
            bucket["latest_sale_at"] = row.get("sale_created_at")

    if normalized_sort_by == "gross_cents":
        sort_key = lambda item: (item["gross_cents"], item["units_sold"], item["sales_count"])
    elif normalized_sort_by == "sales_count":
        sort_key = lambda item: (item["sales_count"], item["units_sold"], item["gross_cents"])
    elif normalized_sort_by == "latest_sale_at":
        sort_key = lambda item: (item["latest_sale_at"] or "", item["gross_cents"], item["units_sold"])
    else:
        sort_key = lambda item: (item["units_sold"], item["gross_cents"], item["sales_count"])

    results = sorted(buckets.values(), key=sort_key, reverse=True)
    return results[:top_n]


def _sales_query_payload(
    sale_rows: list[dict[str, Any]],
    *,
    sale_date: str | None,
    start_date: str | None,
    end_date: str | None,
    card_query: str | None,
    set_code: str | None,
    group_by: str,
    sort_by: str,
    top_n: int,
) -> dict[str, Any]:
    """Shape a sales aggregation response from already-loaded canonical sale rows."""
    normalized_group_by = _normalize_sales_group_by(group_by)
    normalized_sort_by = _normalize_sales_sort_by(sort_by)
    results = summarize_sales_events(
        sale_rows,
        top_n=top_n,
        group_by=normalized_group_by,
        sort_by=normalized_sort_by,
    )
    return {
        "dataset": "silver.sales_events",
        "time_range": _sales_time_range_payload(
            sale_date=sale_date,
            start_date=start_date,
            end_date=end_date,
        ),
        "filters": {
            "card_query": card_query,
            "set_code": set_code,
        },
        "group_by": normalized_group_by,
        "sort_by": normalized_sort_by,
        "sale_row_count": len(sale_rows),
        "result_count": len(results),
        "results": results,
    }


def query_sales(
    client,
    *,
    sale_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    days: int | None = None,
    end_on: str | date | None = None,
    card_query: str | None = None,
    set_code: str | None = None,
    group_by: str = "card",
    sort_by: str = "units_sold",
    top_n: int = 20,
) -> dict[str, Any]:
    """Run a canonical sales query and return both metadata and aggregated rows."""
    resolved_sale_date = sale_date
    resolved_start_date = start_date
    resolved_end_date = end_date

    if days is not None:
        if sale_date or start_date or end_date:
            raise ValueError("Use either explicit date filters or `days`, not both.")
        resolved_start_date, resolved_end_date = _resolve_relative_window(days=days, end_date=end_on)

    sale_rows = scan_sales_events(
        client,
        sale_date=resolved_sale_date,
        start_date=resolved_start_date,
        end_date=resolved_end_date,
        card_query=card_query,
        set_code=set_code,
    )
    return _sales_query_payload(
        sale_rows,
        sale_date=resolved_sale_date,
        start_date=resolved_start_date,
        end_date=resolved_end_date,
        card_query=card_query,
        set_code=set_code,
        group_by=group_by,
        sort_by=sort_by,
        top_n=top_n,
    )


def top_sellers(
    client,
    *,
    days: int = 7,
    end_date: str | date | None = None,
    card_query: str | None = None,
    set_code: str | None = None,
    group_by: str = "variant",
    sort_by: str = "units_sold",
    top_n: int = 20,
) -> dict[str, Any]:
    """Convenience wrapper for the common recent top-sellers question."""
    return query_sales(
        client,
        days=days,
        end_on=end_date,
        card_query=card_query,
        set_code=set_code,
        group_by=group_by,
        sort_by=sort_by,
        top_n=top_n,
    )


def _build_card_price_index(products_data: dict[str, Any], field: str) -> dict[tuple[str, str], float]:
    """Index comparable card-level price fields for snapshot comparisons."""
    index = {}
    for card in iter_cards(products_data):
        key = (card.get("name") or "", card.get("set_code") or "")
        value = card.get(field)
        if value is not None:
            index[key] = float(value)
    return index


def get_price_changes(
    client,
    *,
    limit: int = 20,
    field: str = "price_cents",
    bucket: str = BUCKET,
) -> list[dict[str, Any]]:
    """Compare the latest two raw snapshots and rank absolute price movement."""
    snapshots = list_snapshots(client, limit=2, bucket=bucket)
    if len(snapshots) < 2:
        return []
    newest = load_snapshot_data(client, snapshots[0]["prefix"], "products_singles", bucket=bucket)
    previous = load_snapshot_data(client, snapshots[1]["prefix"], "products_singles", bucket=bucket)
    newest_index = _build_card_price_index(newest, field)
    previous_index = _build_card_price_index(previous, field)
    changes = []
    for key, new_value in newest_index.items():
        old_value = previous_index.get(key)
        if old_value in (None, 0):
            continue
        delta = new_value - old_value
        changes.append(
            {
                "card_name": key[0],
                "set_code": key[1],
                "old_value": old_value,
                "new_value": new_value,
                "delta": delta,
                "pct_change": round((delta / old_value) * 100, 2),
                "field": field,
            }
        )
    changes.sort(key=lambda item: abs(item["pct_change"]), reverse=True)
    return changes[:limit]


def get_card_history(
    client,
    card_query: str,
    *,
    snapshot_limit: int = 10,
    bucket: str = BUCKET,
) -> list[dict[str, Any]]:
    """Track matching cards across recent raw snapshots using card-level fields."""
    snapshots = list_snapshots(client, limit=snapshot_limit, bucket=bucket)
    history = []
    query_lower = card_query.lower()
    for manifest in snapshots:
        products = load_snapshot_data(client, manifest["prefix"], "products_singles", bucket=bucket)
        for card in products.get("data", []):
            if query_lower not in (card.get("name") or "").lower():
                continue
            history.append(
                {
                    "snapshot_ts": manifest["snapshot_ts"],
                    "snapshot_prefix": manifest["prefix"],
                    "card_name": card.get("name"),
                    "set_code": card.get("set_code"),
                    "available_quantity": card.get("available_quantity"),
                    "price_cents": card.get("price_cents"),
                    "price_market": card.get("price_market"),
                    "variant_count": len(card.get("variants", [])),
                    "sales_count": sum(len(variant.get("recent_sales", [])) for variant in card.get("variants", [])),
                }
            )
    history.sort(key=lambda item: item["snapshot_ts"], reverse=True)
    return history


def cents_to_dollars(value: int | float | None) -> float | None:
    """Convert cent-based storage fields into dollar values for display."""
    if value is None:
        return None
    return round(float(value) / 100.0, 2)


def _extract_top_n(question: str, default: int = 20) -> int:
    """Infer `top N` style limits from plain-English questions."""
    match = re.search(r"\btop\s+(\d+)\b", question, flags=re.IGNORECASE)
    if not match:
        return default
    return max(1, int(match.group(1)))


def _extract_quoted_phrase(question: str) -> str | None:
    """Use quoted phrases as high-confidence card-name filters."""
    match = re.search(r'"([^"]+)"', question)
    if match:
        return match.group(1).strip()
    match = re.search(r"'([^']+)'", question)
    if match:
        return match.group(1).strip()
    return None


def _extract_name_query(question: str) -> str | None:
    """Infer a likely card-name filter from either quoted or simple natural phrasing."""
    quoted = _extract_quoted_phrase(question)
    if quoted:
        return quoted

    patterns = [
        r"\bhow many\s+(.+?)\s+sold\b",
        r"\bis\s+(.+?)\s+in stock\b",
        r"\bhistory(?:\s+for)?\s+(.+?)(?=\s+\b(?:on|over|past|last|by|with|in|from|between|today|yesterday|currently|now)\b|[?.!,]|$)",
        r"\b(?:for|of|about)\s+(.+?)(?=\s+\b(?:on|over|past|last|by|with|in|from|between|today|yesterday|currently|now)\b|[?.!,]|$)",
    ]
    for pattern in patterns:
        match = re.search(pattern, question, flags=re.IGNORECASE)
        if not match:
            continue
        candidate = match.group(1).strip(" \"'")
        if candidate and len(candidate) >= 3:
            return candidate
    return None


def _extract_time_range(question: str, *, today: date | None = None) -> dict[str, str] | None:
    """Parse simple absolute or relative time windows from plain-English sales questions."""
    normalized_today = today or date.today()
    iso_dates = re.findall(r"\b\d{4}-\d{2}-\d{2}\b", question)
    if len(iso_dates) >= 2:
        return {"start_date": iso_dates[0], "end_date": iso_dates[1]}
    if len(iso_dates) == 1:
        return {"sale_date": iso_dates[0]}

    relative_days = re.search(r"\b(?:past|last)\s+(\d+)\s+days?\b", question, flags=re.IGNORECASE)
    if relative_days:
        start_date, end_date = _resolve_relative_window(
            days=int(relative_days.group(1)),
            end_date=normalized_today,
        )
        return {"start_date": start_date, "end_date": end_date}

    lower_question = question.lower()
    if "yesterday" in lower_question:
        target = normalized_today - timedelta(days=1)
        return {"sale_date": target.isoformat()}
    if "today" in lower_question:
        return {"sale_date": normalized_today.isoformat()}
    return None


def _infer_sales_group_by(question: str) -> str:
    """Pick a sales grouping grain from simple phrasing cues."""
    lower_question = question.lower()
    if "variant" in lower_question or "sku" in lower_question or "product" in lower_question:
        return "variant"
    if "card" in lower_question:
        return "card"
    return "variant"


def _infer_sales_sort_keys(question: str) -> list[str]:
    """Pick one or more ranking metrics from common sales wording."""
    lower_question = question.lower()
    wants_volume = any(term in lower_question for term in ("volume", "units sold", "most sold"))
    wants_revenue = any(term in lower_question for term in ("revenue", "gross", "sales dollars"))
    if wants_volume and wants_revenue:
        return ["units_sold", "gross_cents"]
    if wants_revenue:
        return ["gross_cents"]
    return ["units_sold"]


def _classify_question(question: str) -> str:
    """Route plain-English questions onto one of the supported data domains."""
    lower_question = question.lower()
    if any(term in lower_question for term in ("sold", "selling", "top seller", "best seller", "revenue", "units sold")):
        return "sales"
    if any(term in lower_question for term in ("available now", "in stock", "inventory", "copies available", "versions of")):
        return "inventory"
    if any(term in lower_question for term in ("price change", "price changes", "what changed", "history")):
        return "snapshot"
    if any(term in lower_question for term in ("schema", "payload", "source data", "original api response")):
        return "source"
    raise ValueError("Could not determine which dataset to use for that question.")


def answer_question(
    question: str,
    *,
    client=None,
    today: date | None = None,
) -> dict[str, Any]:
    """Map common plain-English dataset questions onto explicit helper calls."""
    domain = _classify_question(question)
    active_client = client or get_r2_client()
    lower_question = question.lower()

    if domain == "sales":
        time_range = _extract_time_range(question, today=today)
        if time_range is None:
            start_date, end_date = _resolve_relative_window(days=7, end_date=today or date.today())
            time_range = {"start_date": start_date, "end_date": end_date}
        top_n = _extract_top_n(question)
        group_by = _infer_sales_group_by(question)
        sort_keys = _infer_sales_sort_keys(question)
        card_query = _extract_name_query(question)
        sale_rows = scan_sales_events(
            active_client,
            sale_date=time_range.get("sale_date"),
            start_date=time_range.get("start_date"),
            end_date=time_range.get("end_date"),
            card_query=card_query,
        )
        rankings = []
        for sort_key in sort_keys:
            rankings.append(
                _sales_query_payload(
                    sale_rows,
                    sale_date=time_range.get("sale_date"),
                    start_date=time_range.get("start_date"),
                    end_date=time_range.get("end_date"),
                    card_query=card_query,
                    set_code=None,
                    group_by=group_by,
                    sort_by=sort_key,
                    top_n=top_n,
                )
            )
        return {
            "question": question,
            "domain": "sales",
            "dataset": "silver.sales_events",
            "rankings": rankings,
            "assumptions": [
                "Historical answers come from canonical silver.sales_events.",
                "If no explicit date is provided, the default window is the last 7 days ending today.",
            ],
        }

    if domain == "inventory":
        variant_rows = load_latest_variant_snapshot(active_client)
        results = filter_variant_rows(
            variant_rows,
            name_query=_extract_name_query(question),
            require_sales="with sales" in lower_question,
            limit=_extract_top_n(question),
        )
        latest = get_latest_snapshot(active_client)
        return {
            "question": question,
            "domain": "inventory",
            "dataset": "silver.products_singles_variants",
            "snapshot_ts": latest["snapshot_ts"],
            "result_count": len(results),
            "results": results,
        }

    if domain == "snapshot":
        if "history" in lower_question:
            card_query = _extract_name_query(question)
            if not card_query:
                raise ValueError("Snapshot history questions should quote the card name.")
            return {
                "question": question,
                "domain": "snapshot",
                "dataset": "raw snapshots + snapshot-partitioned Parquet",
                "results": get_card_history(active_client, card_query, snapshot_limit=_extract_top_n(question, default=10)),
            }
        return {
            "question": question,
            "domain": "snapshot",
            "dataset": "raw snapshots",
            "results": get_price_changes(active_client, limit=_extract_top_n(question, default=20)),
        }

    return {
        "question": question,
        "domain": "source",
        "dataset": "raw snapshots",
        "results": describe_snapshot_schema(active_client),
    }


def _pretty_print(payload: Any) -> None:
    """Print stable JSON for CLI inspection."""
    print(json.dumps(payload, indent=2, default=str))


def build_cli() -> argparse.ArgumentParser:
    """Create the explicit CLI parser for raw and Parquet queries."""
    parser = argparse.ArgumentParser(description="Mana Pool R2 data agent")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("latest", help="Show the latest raw snapshot manifest")

    snapshots_parser = subparsers.add_parser("snapshots", help="List recent raw snapshots")
    snapshots_parser.add_argument("--limit", type=int, default=10)

    schema_parser = subparsers.add_parser("schema", help="Describe the current raw snapshot schema")
    schema_parser.add_argument("--prefix", type=str, default=None)

    search_parser = subparsers.add_parser("search", help="Search cards by name in the latest raw products snapshot")
    search_parser.add_argument("query", type=str)
    search_parser.add_argument("--limit", type=int, default=20)

    variants_parser = subparsers.add_parser("variants", help="Query the latest silver variant snapshot explicitly")
    variants_parser.add_argument("--name-query", type=str, default=None)
    variants_parser.add_argument("--set-code", type=str, default=None)
    variants_parser.add_argument("--language-id", type=str, default=None)
    variants_parser.add_argument("--condition-id", type=str, default=None)
    variants_parser.add_argument("--finish-id", type=str, default=None)
    variants_parser.add_argument("--min-price", type=int, default=None)
    variants_parser.add_argument("--max-price", type=int, default=None)
    variants_parser.add_argument("--min-quantity", type=int, default=None)
    variants_parser.add_argument("--require-sales", action="store_true")
    variants_parser.add_argument("--limit", type=int, default=20)

    sales_events_parser = subparsers.add_parser("sales-events", help="Scan canonical sales events by explicit date filters")
    sales_events_parser.add_argument("--sale-date", type=str, default=None)
    sales_events_parser.add_argument("--start-date", type=str, default=None)
    sales_events_parser.add_argument("--end-date", type=str, default=None)
    sales_events_parser.add_argument("--card-query", type=str, default=None)
    sales_events_parser.add_argument("--set-code", type=str, default=None)
    sales_events_parser.add_argument("--limit", type=int, default=None)

    sales_summary_parser = subparsers.add_parser("sales-summary", help="Summarize canonical sales events by date range")
    sales_summary_parser.add_argument("--sale-date", type=str, default=None)
    sales_summary_parser.add_argument("--start-date", type=str, default=None)
    sales_summary_parser.add_argument("--end-date", type=str, default=None)
    sales_summary_parser.add_argument("--card-query", type=str, default=None)
    sales_summary_parser.add_argument("--set-code", type=str, default=None)
    sales_summary_parser.add_argument("--group-by", choices=["card", "variant"], default="card")
    sales_summary_parser.add_argument("--sort-by", choices=["units_sold", "gross_cents", "sales_count", "latest_sale_at"], default="units_sold")
    sales_summary_parser.add_argument("--top", type=int, default=20)

    top_sellers_parser = subparsers.add_parser("top-sellers", help="Show top sellers over a recent relative window")
    top_sellers_parser.add_argument("--days", type=int, default=7)
    top_sellers_parser.add_argument("--end-date", type=str, default=None)
    top_sellers_parser.add_argument("--card-query", type=str, default=None)
    top_sellers_parser.add_argument("--set-code", type=str, default=None)
    top_sellers_parser.add_argument("--group-by", choices=["card", "variant"], default="variant")
    top_sellers_parser.add_argument("--sort-by", choices=["units_sold", "gross_cents", "sales_count", "latest_sale_at"], default="units_sold")
    top_sellers_parser.add_argument("--top", type=int, default=20)

    answer_parser = subparsers.add_parser("answer", help="Route a plain-English question onto the query helpers")
    answer_parser.add_argument("question", type=str)

    price_changes_parser = subparsers.add_parser("price-changes", help="Compare the latest two raw snapshots")
    price_changes_parser.add_argument("--limit", type=int, default=20)
    price_changes_parser.add_argument("--field", type=str, default="price_cents")

    history_parser = subparsers.add_parser("history", help="Show card history across raw snapshots")
    history_parser.add_argument("query", type=str)
    history_parser.add_argument("--limit", type=int, default=10)

    return parser


def main() -> None:
    """Run the explicit CLI entrypoint."""
    parser = build_cli()
    args = parser.parse_args()
    client = get_r2_client()

    if args.command == "latest":
        _pretty_print(get_latest_snapshot(client))
        return

    if args.command == "snapshots":
        _pretty_print(list_snapshots(client, limit=args.limit))
        return

    if args.command == "schema":
        _pretty_print(describe_snapshot_schema(client, prefix=args.prefix))
        return

    if args.command == "search":
        products = load_latest_dataset(client, "products_singles")
        _pretty_print(search_cards(products, args.query, limit=args.limit))
        return

    if args.command == "variants":
        variant_rows = load_latest_variant_snapshot(client)
        _pretty_print(
            filter_variant_rows(
                variant_rows,
                name_query=args.name_query,
                set_code=args.set_code,
                language_id=args.language_id,
                condition_id=args.condition_id,
                finish_id=args.finish_id,
                min_price=args.min_price,
                max_price=args.max_price,
                min_quantity=args.min_quantity,
                require_sales=args.require_sales,
                limit=args.limit,
            )
        )
        return

    if args.command == "sales-events":
        _pretty_print(
            scan_sales_events(
                client,
                sale_date=args.sale_date,
                start_date=args.start_date,
                end_date=args.end_date,
                card_query=args.card_query,
                set_code=args.set_code,
                limit=args.limit,
            )
        )
        return

    if args.command == "sales-summary":
        _pretty_print(
            query_sales(
                client,
                sale_date=args.sale_date,
                start_date=args.start_date,
                end_date=args.end_date,
                card_query=args.card_query,
                set_code=args.set_code,
                group_by=args.group_by,
                sort_by=args.sort_by,
                top_n=args.top,
            )
        )
        return

    if args.command == "top-sellers":
        _pretty_print(
            top_sellers(
                client,
                days=args.days,
                end_date=args.end_date,
                card_query=args.card_query,
                set_code=args.set_code,
                group_by=args.group_by,
                sort_by=args.sort_by,
                top_n=args.top,
            )
        )
        return

    if args.command == "answer":
        _pretty_print(answer_question(args.question, client=client))
        return

    if args.command == "price-changes":
        _pretty_print(get_price_changes(client, limit=args.limit, field=args.field))
        return

    if args.command == "history":
        _pretty_print(get_card_history(client, args.query, snapshot_limit=args.limit))
        return


if __name__ == "__main__":
    main()
