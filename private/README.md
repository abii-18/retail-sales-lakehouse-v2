# Retail Sales Lakehouse — Source Database Kit (v3, final)

Tested end-to-end on a real Postgres instance at full scale: 10,000 customers,
2,000 products, 50 stores, 100,000 orders, ~234,000 order_items, 100,000
payments → ~64MB total (well inside AWS S3 free-tier storage and negligible
for Snowflake storage credits).

## v3 changes from v2
- **`customer_id` on `orders` reverted to `NOT NULL`** — the operational source
  system enforces its own referential integrity; a "missing customer_id" case
  belongs downstream (a staging table or a failed join), not baked into the
  OLTP schema.
- **Product popularity now follows a tuned Zipf distribution** (`PRODUCT_ZIPF_S
  = 0.82` in `generate_data.py`), not uniform random selection. Audited
  against the actual generated data:
  | Metric | Target | Actual |
  |---|---|---|
  | Top 1% of SKUs' share of order_items | ~25% | 27.1% |
  | Top 5% of SKUs' share of order_items | ~50% | 46.1% |
  | Bottom 50% of SKUs' share of order_items | ~15% | 14.8% |
  Popularity rank is derived from a **stable hash of `product_id`**, so the
  same products stay "bestsellers" across the initial load and every
  subsequent daily batch — not re-randomized each run.
- **Store-size tiers added** (`store_size`: large/medium/small, new column on
  `stores`). Large stores (15% of stores) get a 3x order-volume multiplier,
  medium (45%) get 1.5x, small (40%) get 1.0x — combined multiplicatively
  with the existing regional performance weight. Audited: large stores
  averaged ~4,164 orders/store vs. ~1,049/store for small stores.

## Setup (run in your own Docker environment, not this sandbox)
1. Add a `postgres` service to your `docker-compose.yml` next to Airflow.
2. Apply schema: `psql -h <host> -U <user> -d retail_lakehouse -f schema.sql`
3. `pip install faker psycopg2-binary`
4. One-time bulk load:
   `PGHOST=... PGUSER=... PGPASSWORD=... PGDATABASE=retail_lakehouse python generate_data.py --mode initial`
   (~20-30 seconds, produces `dirty_data_manifest.csv` in your working dir)
5. Simulate each subsequent day of business:
   `python generate_data.py --mode daily --orders 2500`
   Appends orders dated "today," nudges a handful of existing
   customers/products (SCD1 test cases), keeps the same bestseller products
   dominant.

## Full feature set (cumulative)
- Referential integrity enforced by real FK constraints (verified: 0 orphans)
- Skewed category mix: Electronics ~40%, Apparel ~30%, Home & Kitchen ~20%,
  Grocery/Beauty ~10% combined
- Pareto-shaped orders-per-customer: ~65% light, ~22% regular, ~6% frequent,
  ~2% VIP (up to 200+ orders), ~5% permanently dormant (zero orders)
- Seasonality: December holiday spike (~2x), weekend bump (~60% higher than
  weekdays), gentle month-over-month growth across a 12-month window
- Category-based average order value (Electronics lines ~₹5,000 avg vs.
  Grocery ~₹200 avg — ~24x spread)
- Regional store performance bias (independent of size tier)
- ~3% of products marked permanently out-of-stock (`is_active = false`)
- `dirty_data_manifest.csv`: every dirty row logged with table, a row hint,
  the specific field, and the exact issue — a ground truth for your
  Silver-layer validation tests
  - Note: `row_hint` is a generation-sequence reference (e.g. `row#8`), not
    the final DB id, since Postgres assigns ids via `SERIAL` after the row
    is built. Ask if you want the manifest re-keyed by actual DB ids.

## Design notes worth remembering for interviews
- `order_items.unit_price_at_sale` / `unit_cost_at_sale` are snapshots taken
  at insert time — not live joins to `products` — so historical profit never
  drifts if a product's cost changes later.
- `payments` is a bronze/source table, deliberately excluded from
  `FACT_SALES` grain in V1 (per your locked scope decision) — a Phase 2 hook.
- Currency conversion is intentionally NOT done in Postgres — that's the
  Spark transform layer's job, joining against the exchange-rate API.
- Product popularity is Zipf-distributed with a **stable** ranking (hash of
  product_id), so your "top products" Gold mart will show consistent
  bestsellers day over day — exactly like a real retailer's sales data.
