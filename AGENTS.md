# Mana Pool R2 Agent

Schema-aware agent guidance for querying Mana Pool snapshot data stored in Cloudflare R2.

The project now uses two storage layers:
- Raw JSON snapshots under `snapshots/...` remain the immutable source of truth.
- Parquet datasets under `parquet/...` are the analytical/query surface.

## Setup

Install dependencies:
```bash
pip install boto3 python-dotenv pyarrow requests duckdb
```

Set environment variables in `.env`:
```env
R2_ACCOUNT_ID=your_account_id
R2_ACCESS_KEY_ID=your_access_key
R2_SECRET_ACCESS_KEY=your_secret_key
R2_BUCKET=manapool-snapshots
```

## Storage Model

Raw snapshot layout:

```text
snapshots/YYYY-MM-DD/YYYYMMDDTHHMMSSZ/
  manifest.json.gz
  prices_singles.json.gz
  prices_sealed.json.gz
  products_singles.json.gz
```

Derived Parquet layout:

```text
parquet/
  bronze/
    manifests/
    prices_singles_rows/
    prices_sealed_rows/
    products_singles_cards_raw/
  silver/
    products_singles_variants/
    sales_events/
```

Important data rules:
- Bronze tables are 1:1 mirrors of the JSON payloads plus provenance columns.
- `silver.products_singles_variants` is the query surface for current inventory questions.
- `silver.sales_events` is the canonical historical sales table.
- `silver.sales_events` is deduped by `created_at` across carried-over snapshot files.
- `silver.sales_events` is partitioned by `sale_year/sale_month/sale_day` so DuckDB can prune date scans.
- Monetary values remain in cents unless you explicitly convert them.

## Query Recipes

Use these rules to ground dataset choice. For common plain-English questions, prefer
`answer_question(...)`, which routes onto these explicit helpers rather than inventing
new aggregation logic inline.

### Current Inventory

Use the latest `silver.products_singles_variants` partition when the user asks:
- what copies are available now
- inventory filters by language, condition, finish, or price
- which versions of a card currently exist

Preferred helpers in `mp_r2_agent.py`:
- `answer_question(question, client=...)`
- `get_latest_snapshot(client)`
- `load_latest_variant_snapshot(client)`
- `filter_variant_rows(...)`

### Historical Sales

Use `silver.sales_events` when the user asks:
- what sold on a specific date
- top sellers over a date range
- units sold or gross revenue by card or variant
- best-selling cards on a day like `2026-03-13`

Preferred helpers:
- `answer_question(question, client=...)`
- `query_sales(client, start_date=..., end_date=..., group_by=..., sort_by=...)`
- `top_sellers(client, days=..., group_by=..., sort_by=...)`
- `scan_sales_events(client, sale_date=..., start_date=..., end_date=...)`
- `summarize_sales_events(...)`

`scan_sales_events(...)` is backed by DuckDB over Parquet rather than sequential object reads.

When the user asks for rankings such as "top selling products over the past 7 days by
volume and by revenue", prefer `answer_question(...)` or `top_sellers(...)` rather than
writing a custom aggregation script in the turn.

Never answer historical sales questions from only the latest `products_singles` snapshot.

### Snapshot Comparisons

Use raw snapshot files or snapshot-partitioned Parquet tables when the user asks:
- price changes between snapshots
- what changed from one run to another
- how inventory or pricing moved by snapshot timestamp

Preferred helpers:
- `answer_question(question, client=...)`
- `list_snapshots(client, limit=...)`
- `load_snapshot_data(client, prefix, dataset)`
- `load_snapshot_table_rows(client, table_name, manifest)`
- `get_price_changes(...)`
- `get_card_history(...)`

### Source Verification

Use raw JSON or bronze mirrors when the user asks:
- what the original payload looked like
- whether a derived answer matches source data
- how a specific field was represented in the original API response

Preferred helpers:
- `answer_question(question, client=...)`
- `load_snapshot_data(...)`
- `describe_snapshot_schema(...)`
- `load_snapshot_table_rows(client, "products_singles_cards_raw", manifest)`

## Core Functions

`mp_r2_agent.py` exposes these main entry points:

- `answer_question(question, client=None, today=None)`
- `get_r2_client()`
- `get_latest_snapshot(client)`
- `list_snapshots(client, limit=10)`
- `load_snapshot_data(client, prefix, dataset)`
- `load_latest_dataset(client, dataset)`
- `describe_snapshot_schema(client, prefix=None)`
- `iter_cards(products_data)`
- `iter_variant_rows(products_data)`
- `search_cards(data, query, limit=20)`
- `filter_variant_rows(variant_rows, ...)`
- `load_snapshot_table_rows(client, table_name, manifest)`
- `load_latest_variant_snapshot(client)`
- `scan_sales_events(client, sale_date=None, start_date=None, end_date=None, ...)`
- `summarize_sales_events(sale_rows, top_n=20, group_by="card", sort_by="units_sold")`
- `query_sales(client, sale_date=None, start_date=None, end_date=None, ..., group_by="card", sort_by="units_sold")`
- `top_sellers(client, days=7, group_by="variant", sort_by="units_sold")`
- `get_price_changes(client, limit=20, field="price_cents")`
- `get_card_history(client, card_query, snapshot_limit=10)`
- `cents_to_dollars(value)`

## Usage via opencode

```python
from task import Task

Task(
    command="What was the best selling card on 2026-03-13?",
    description="Query Mana Pool snapshot data",
    prompt="""You are the Mana Pool R2 data agent.

Use `mp_r2_agent.py` to answer the user's question from live R2 snapshot data.

Rules:
- Start by creating a client with `get_r2_client()`.
- Prefer `answer_question(...)` first for plain-English questions. If it cannot express the
  question cleanly, fall back to the explicit helpers below.
- For current inventory, use the latest `silver.products_singles_variants` partition.
- For historical sales, use canonical `silver.sales_events`.
- For snapshot comparisons, use raw snapshots or snapshot-partitioned Parquet tables.
- State the dataset, time range, and grouping used in the answer.
- Treat money fields as cents unless you explicitly convert them.

Return a concise answer with the result, assumptions, and key metrics used.""",
    subagent_type="explore",
)
```

## CLI Usage

```bash
# Show the latest raw manifest
python mp_r2_agent.py latest

# List recent raw snapshots
python mp_r2_agent.py snapshots --limit 5

# Describe the raw snapshot schema
python mp_r2_agent.py schema

# Search card names in the latest raw products snapshot
python mp_r2_agent.py search "Lightning Bolt" --limit 10

# Query the latest variant snapshot explicitly
python mp_r2_agent.py variants --name-query "Lightning Bolt" --language-id EN --condition-id LP --require-sales

# Scan canonical sales events for a day
python mp_r2_agent.py sales-events --sale-date 2026-03-13 --card-query "Lightning Bolt"

# Summarize canonical sales over a date range
python mp_r2_agent.py sales-summary --start-date 2026-03-13 --end-date 2026-03-14 --top 20

# Summarize canonical sales over a date range by revenue
python mp_r2_agent.py sales-summary --start-date 2026-03-13 --end-date 2026-03-14 --group-by variant --sort-by gross_cents --top 20

# Show recent top sellers over a relative window
python mp_r2_agent.py top-sellers --days 7 --group-by variant --sort-by units_sold --top 10

# Route a plain-English question onto the explicit query helpers
python mp_r2_agent.py answer "What were the top selling products over the past 7 days by revenue?"

# Compare latest two raw snapshots
python mp_r2_agent.py price-changes --limit 20

# Show raw snapshot history for a card
python mp_r2_agent.py history "Black Lotus" --limit 10
```
