# Mana Pool Parquet Schema

This document defines the Parquet layout for Mana Pool snapshot analytics.

Design rules:
- Raw JSON snapshots in `snapshots/...` remain the source of truth.
- Bronze Parquet tables preserve the JSON payloads 1:1, with added provenance columns prefixed by `_`.
- Silver Parquet tables flatten nested records for queryability, while still carrying direct source fields.
- Canonical sales uniqueness is enforced with `_sale_id == sale_created_at == created_at`.
- Monetary values remain in cents to preserve the raw API semantics.
- `sales_events` uses hive-style `sale_year/sale_month/sale_day` partitions so DuckDB can scan only the needed dates.

## Storage Layout

```text
parquet/
  bronze/
    manifests/
      snapshot_date=YYYY-MM-DD/
        snapshot_ts=YYYYMMDDTHHMMSSZ/
          part-00000.parquet
    prices_singles_rows/
      snapshot_date=YYYY-MM-DD/
        snapshot_ts=YYYYMMDDTHHMMSSZ/
          part-00000.parquet
    prices_sealed_rows/
      snapshot_date=YYYY-MM-DD/
        snapshot_ts=YYYYMMDDTHHMMSSZ/
          part-00000.parquet
    products_singles_cards_raw/
      snapshot_date=YYYY-MM-DD/
        snapshot_ts=YYYYMMDDTHHMMSSZ/
          part-00000.parquet
  silver/
    products_singles_variants/
      snapshot_date=YYYY-MM-DD/
        snapshot_ts=YYYYMMDDTHHMMSSZ/
          part-00000.parquet
    sales_events/
      sale_year=YYYY/
        sale_month=MM/
          sale_day=DD/
            part-YYYYMMDDTHHMMSSZ.parquet
```

## Shared Provenance Columns

These columns are present on every bronze and silver row:

- `_snapshot_date: string`
- `_snapshot_ts_partition: string`
- `_snapshot_prefix: string`
- `_source_key: string`
- `_ingested_at: string`

Silver sales rows also add:

- `_sale_id: string`
- `_sale_date: string`
- `_first_seen_snapshot_ts: string`
- `_first_seen_snapshot_prefix: string`

## Bronze Tables

### `bronze.manifests`

One row per `manifest.json.gz`.

Columns:
- `_snapshot_date: string`
- `_snapshot_ts_partition: string`
- `_snapshot_prefix: string`
- `_source_key: string`
- `_ingested_at: string`
- `snapshot_ts: string`
- `prefix: string`
- `files: struct`
  - `prices_singles: string`
  - `prices_sealed: string`
  - `products_singles: string` nullable
- `stats: struct`
  - `singles_count: int64`
  - `singles_total_qty: int64`
  - `sealed_count: int64`
  - `sealed_total_qty: int64`
  - `singles_fetch_s: double`
  - `sealed_fetch_s: double`
  - `sales_fetch_s: double` nullable
  - `sales_concurrency: int64` nullable
  - `cards_with_sales_data: int64` nullable
  - `total_variants: int64` nullable
  - `total_sale_records: int64` nullable
  - `batches_total: int64` nullable
  - `batches_failed: int64` nullable
- `raw_json: string`

### `bronze.prices_singles_rows`

One row per `prices_singles.data[]`.

Columns:
- `_snapshot_date: string`
- `_snapshot_ts_partition: string`
- `_snapshot_prefix: string`
- `_source_key: string`
- `_ingested_at: string`
- `meta: struct`
  - `as_of: string`
  - `base_url: string` nullable
- `name: string`
- `set_code: string`
- `number: string`
- `multiverse_id: string` nullable
- `scryfall_id: string` nullable
- `available_quantity: int64` nullable
- `price_cents: int64` nullable
- `price_cents_lp_plus: int64` nullable
- `price_cents_nm: int64` nullable
- `price_cents_foil: int64` nullable
- `price_cents_lp_plus_foil: int64` nullable
- `price_cents_nm_foil: int64` nullable
- `price_cents_etched: int64` nullable
- `price_cents_lp_plus_etched: int64` nullable
- `price_cents_nm_etched: int64` nullable
- `price_market: int64` nullable
- `price_market_foil: int64` nullable
- `url: string`
- `raw_json: string`

### `bronze.prices_sealed_rows`

One row per `prices_sealed.data[]`.

Columns:
- `_snapshot_date: string`
- `_snapshot_ts_partition: string`
- `_snapshot_prefix: string`
- `_source_key: string`
- `_ingested_at: string`
- `meta: struct`
  - `as_of: string`
  - `base_url: string` nullable
- `product_type: string`
- `product_id: string`
- `set_code: string`
- `name: string`
- `tcgplayer_product_id: int64` nullable
- `language_id: string` nullable
- `low_price: int64` nullable
- `available_quantity: int64` nullable
- `price_market: int64` nullable
- `url: string`
- `raw_json: string`

### `bronze.products_singles_cards_raw`

One row per `products_singles.data[]` card object, with nested variants preserved.

Columns:
- `_snapshot_date: string`
- `_snapshot_ts_partition: string`
- `_snapshot_prefix: string`
- `_source_key: string`
- `_ingested_at: string`
- `meta: struct`
  - `snapshot_ts: string`
  - `cards_requested: int64`
  - `cards_returned: int64`
  - `batches_total: int64`
  - `batches_failed: int64`
  - `concurrency: int64`
  - `fetch_duration_s: double`
- `url: string`
- `name: string`
- `set_code: string`
- `number: string`
- `multiverse_id: string` nullable
- `scryfall_id: string` nullable
- `tcgplayer_product_id: int64` nullable
- `available_quantity: int64` nullable
- `price_cents: int64` nullable
- `price_cents_lp_plus: int64` nullable
- `price_cents_nm: int64` nullable
- `price_cents_foil: int64` nullable
- `price_cents_lp_plus_foil: int64` nullable
- `price_cents_nm_foil: int64` nullable
- `price_cents_etched: int64` nullable
- `price_cents_lp_plus_etched: int64` nullable
- `price_cents_nm_etched: int64` nullable
- `price_market: int64` nullable
- `price_market_foil: int64` nullable
- `variants: list<struct>`
  - `product_type: string`
  - `product_id: string`
  - `tcgplayer_sku_id: int64` nullable
  - `language_id: string` nullable
  - `condition_id: string` nullable
  - `finish_id: string` nullable
  - `low_price: int64` nullable
  - `available_quantity: int64` nullable
  - `recent_sales: list<struct>`
    - `created_at: string`
    - `price: int64` nullable
    - `quantity: int64` nullable
- `raw_json: string`

## Silver Tables

### `silver.products_singles_variants`

One row per `products_singles.data[*].variants[*]`.

Columns:
- `_snapshot_date: string`
- `_snapshot_ts_partition: string`
- `_snapshot_prefix: string`
- `_source_key: string`
- `_ingested_at: string`
- `card_url: string`
- `card_name: string`
- `set_code: string`
- `number: string`
- `multiverse_id: string` nullable
- `scryfall_id: string` nullable
- `tcgplayer_product_id: int64` nullable
- `card_available_quantity: int64` nullable
- `card_price_cents: int64` nullable
- `card_price_cents_lp_plus: int64` nullable
- `card_price_cents_nm: int64` nullable
- `card_price_cents_foil: int64` nullable
- `card_price_cents_lp_plus_foil: int64` nullable
- `card_price_cents_nm_foil: int64` nullable
- `card_price_cents_etched: int64` nullable
- `card_price_cents_lp_plus_etched: int64` nullable
- `card_price_cents_nm_etched: int64` nullable
- `card_price_market: int64` nullable
- `card_price_market_foil: int64` nullable
- `variant_product_type: string`
- `variant_product_id: string`
- `variant_tcgplayer_sku_id: int64` nullable
- `variant_language_id: string` nullable
- `variant_condition_id: string` nullable
- `variant_finish_id: string` nullable
- `variant_low_price: int64` nullable
- `variant_available_quantity: int64` nullable
- `recent_sales: list<struct>`
  - `created_at: string`
  - `price: int64` nullable
  - `quantity: int64` nullable
- `recent_sales_count: int64`
- `variant_raw_json: string`

### `silver.sales_events`

One row per canonical sale event. These rows are deduped by `created_at` across carried-over snapshot files.

Columns:
- `_sale_id: string`
  - exact value of `sale_created_at`
- `_sale_date: string`
- `_first_seen_snapshot_ts: string`
- `_first_seen_snapshot_prefix: string`
- `_snapshot_date: string`
- `_snapshot_ts_partition: string`
- `_snapshot_prefix: string`
- `_source_key: string`
- `_ingested_at: string`
- `card_url: string`
- `card_name: string`
- `set_code: string`
- `number: string`
- `multiverse_id: string` nullable
- `scryfall_id: string` nullable
- `tcgplayer_product_id: int64` nullable
- `card_available_quantity: int64` nullable
- `card_price_cents: int64` nullable
- `card_price_cents_lp_plus: int64` nullable
- `card_price_cents_nm: int64` nullable
- `card_price_cents_foil: int64` nullable
- `card_price_cents_lp_plus_foil: int64` nullable
- `card_price_cents_nm_foil: int64` nullable
- `card_price_cents_etched: int64` nullable
- `card_price_cents_lp_plus_etched: int64` nullable
- `card_price_cents_nm_etched: int64` nullable
- `card_price_market: int64` nullable
- `card_price_market_foil: int64` nullable
- `variant_product_type: string`
- `variant_product_id: string`
- `variant_tcgplayer_sku_id: int64` nullable
- `variant_language_id: string` nullable
- `variant_condition_id: string` nullable
- `variant_finish_id: string` nullable
- `variant_low_price: int64` nullable
- `variant_available_quantity: int64` nullable
- `sale_created_at: string`
- `sale_price: int64` nullable
- `sale_quantity: int64` nullable
- `sale_raw_json: string`

## Canonical Query Rules

- Current inventory questions should query the latest `silver.products_singles_variants` partition.
- Historical sales questions should query `silver.sales_events`.
- Snapshot comparison questions should query bronze or silver snapshot partitions directly.
- Source verification questions should read bronze mirrors or raw JSON snapshots.
