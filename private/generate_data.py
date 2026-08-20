"""
Retail Sales Lakehouse - Data Generator (v2 - business-realistic)

Two modes:
  --mode initial   -> one-time bulk load
  --mode daily      -> simulates one day of business + SCD1-style updates

Business realism built in (not pure-random Faker):
  - Skewed category mix (Electronics/Apparel dominate, matching real retail)
  - Pareto-shaped orders-per-customer (most customers light, a few VIP)
  - Weekend + December holiday order-volume spikes
  - Regional store performance bias
  - A small % of permanently inactive customers and out-of-stock products
  - A dirty_data_manifest.csv naming exactly which rows are bad and why

Usage:
  python generate_data.py --mode initial
  python generate_data.py --mode daily --orders 2500

Connection settings read from env vars: PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
"""

import os
import csv
import random
import hashlib
import itertools
import argparse
from datetime import datetime, timedelta

import psycopg2
import psycopg2.extras
from faker import Faker

fake = Faker()

# ---------------------------------------------------------------
# Config
# ---------------------------------------------------------------
N_CUSTOMERS = 10_000
N_PRODUCTS = 2_000
N_STORES = 50
N_INITIAL_ORDERS = 100_000
PAYMENT_RATE = 1.0

# category -> (weight, sub_categories, cost_range)  -- weight drives realistic mix,
# cost_range drives category-based average-order-value differences (Electronics >> Grocery)
CATEGORY_CONFIG = {
    "Electronics":    {"weight": 0.40, "subs": ["Mobiles", "Laptops", "Accessories", "Audio"], "cost": (300, 2500)},
    "Apparel":        {"weight": 0.30, "subs": ["Men", "Women", "Kids", "Footwear"],            "cost": (20, 150)},
    "Home & Kitchen":  {"weight": 0.20, "subs": ["Furniture", "Cookware", "Decor"],              "cost": (50, 800)},
    "Grocery":        {"weight": 0.06, "subs": ["Snacks", "Beverages", "Staples"],               "cost": (5, 60)},
    "Beauty":         {"weight": 0.04, "subs": ["Skincare", "Haircare", "Makeup"],               "cost": (10, 120)},
}
CATEGORY_NAMES = list(CATEGORY_CONFIG.keys())
CATEGORY_WEIGHTS = [CATEGORY_CONFIG[c]["weight"] for c in CATEGORY_NAMES]

REGIONS = ["North", "South", "East", "West", "Central"]
# Some regions genuinely outperform others -> regional performance variance in Gold marts
REGION_PERFORMANCE_WEIGHT = {"North": 1.4, "South": 1.2, "West": 1.0, "East": 0.8, "Central": 0.6}

# Store size tier -> (proportion of stores, order-volume multiplier)
# A flagship 'large' store just does more business than a 'small' outlet, independent of region.
STORE_SIZE_CONFIG = {
    "large":  {"weight": 0.15, "order_multiplier": 3.0},
    "medium": {"weight": 0.45, "order_multiplier": 1.5},
    "small":  {"weight": 0.40, "order_multiplier": 1.0},
}
STORE_SIZE_NAMES = list(STORE_SIZE_CONFIG.keys())
STORE_SIZE_PROPORTIONS = [STORE_SIZE_CONFIG[s]["weight"] for s in STORE_SIZE_NAMES]

# Zipf skew for product popularity: rank 1 product is dramatically more popular
# than rank 2000. s=1.0 is classic Zipf ("long tail" e-commerce sales pattern).
PRODUCT_ZIPF_S = 0.82

CURRENCIES = ["INR", "USD", "EUR", "GBP"]
PAYMENT_METHODS = ["credit_card", "upi", "paypal", "net_banking", "debit_card"]
STATE_VARIANTS = ["TN", "Tamil Nadu", "TamilNadu", "KA", "Karnataka", "MH", "Maharashtra"]

# Customer order-frequency segments -> Pareto-shaped orders-per-customer
# (most customers light, a meaningful chunk frequent, a small VIP tail, a few dormant)
CUSTOMER_SEGMENTS = [
    ("dormant", 0.05, 0.0),    # never order again -> "a few inactive customers"
    ("light",   0.65, 1.0),
    ("regular", 0.22, 4.0),
    ("frequent", 0.06, 12.0),
    ("vip",      0.02, 60.0),
]

DB_CONF = dict(
    host=os.environ.get("PGHOST", "127.0.0.1"),
    port=os.environ.get("PGPORT", "5432"),
    dbname=os.environ.get("PGDATABASE", "retail_lakehouse"),
    user=os.environ.get("PGUSER", "retail_user"),
    password=os.environ.get("PGPASSWORD", "retail_pass"),
)

DIRTY_MANIFEST_PATH = os.environ.get("DIRTY_MANIFEST_PATH", "dirty_data_manifest.csv")
_dirty_rows = []  # collected during generation, flushed to CSV at the end


def log_dirty(table, identifier_hint, field, issue):
    _dirty_rows.append({"table": table, "row_hint": identifier_hint, "field": field, "issue": issue})


def flush_dirty_manifest(mode):
    if not _dirty_rows:
        return
    write_header = not os.path.exists(DIRTY_MANIFEST_PATH)
    with open(DIRTY_MANIFEST_PATH, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["table", "row_hint", "field", "issue", "generated_in_mode"])
        if write_header:
            w.writeheader()
        for row in _dirty_rows:
            row["generated_in_mode"] = mode
            w.writerow(row)
    print(f"  -> {len(_dirty_rows)} dirty-data rows logged to {DIRTY_MANIFEST_PATH}")
    _dirty_rows.clear()


def get_conn():
    return psycopg2.connect(**DB_CONF)


# ---------------------------------------------------------------
# CUSTOMERS  (with segment weight + dirty-data tracking)
# ---------------------------------------------------------------
def generate_customers(n):
    rows = []
    segment_names = [s[0] for s in CUSTOMER_SEGMENTS]
    segment_weights = [s[1] for s in CUSTOMER_SEGMENTS]
    for i in range(n):
        segment = random.choices(segment_names, weights=segment_weights, k=1)[0]
        is_active = segment != "dormant"

        email = fake.unique.email()
        if random.random() < 0.03:
            email = None
            log_dirty("customers", f"row#{i} ({fake.first_name()} generated)", "email", "null email")

        phone = fake.phone_number()[:20]
        if random.random() < 0.05:
            phone = None
            log_dirty("customers", f"row#{i}", "phone", "missing phone number")

        state = random.choice(STATE_VARIANTS)
        signup = fake.date_between(start_date="-3y", end_date="-30d")
        rows.append((
            fake.first_name(), fake.last_name(), email, phone,
            fake.street_address()[:200], fake.city()[:100], state,
            "India", signup, is_active, segment,
        ))
    return rows


# ---------------------------------------------------------------
# PRODUCTS  (category-weighted, category-specific cost bands, out-of-stock)
# ---------------------------------------------------------------
def generate_products(n):
    rows = []
    for i in range(n):
        category = random.choices(CATEGORY_NAMES, weights=CATEGORY_WEIGHTS, k=1)[0]
        cfg = CATEGORY_CONFIG[category]
        sub_category = random.choice(cfg["subs"])
        cost = round(random.uniform(*cfg["cost"]), 2)

        if random.random() < 0.01:
            log_dirty("products", f"row#{i}", "unit_cost", "negative cost")
            cost = round(-abs(cost), 2)

        margin = random.uniform(1.15, 1.8)
        price = round(cost * margin, 2) if cost > 0 else round(random.uniform(*cfg["cost"]) * 1.4, 2)

        if random.random() < 0.01:
            log_dirty("products", f"row#{i}", "unit_price", "negative price")
            price = round(-abs(price), 2)

        # ~3% out-of-stock (permanently inactive products)
        is_active = random.random() > 0.03
        if not is_active:
            log_dirty("products", f"row#{i}", "is_active", "marked out-of-stock (inactive)")

        rows.append((fake.catch_phrase()[:150], category, sub_category, price, cost, is_active))
    return rows


def generate_stores(n):
    rows = []
    types = ["physical", "online"]
    for _ in range(n):
        store_type = random.choices(types, weights=[0.7, 0.3])[0]
        store_size = random.choices(STORE_SIZE_NAMES, weights=STORE_SIZE_PROPORTIONS, k=1)[0]
        region = random.choice(REGIONS)
        opened = fake.date_between(start_date="-5y", end_date="-1y")
        rows.append((
            f"{fake.city()} {store_type.title()} Store", store_type, store_size, region, "India", opened,
        ))
    return rows


# ---------------------------------------------------------------
# SEASONALITY: build a weighted pool of dates for a window
# (weekend bump + December holiday spike + gentle month-over-month growth)
# ---------------------------------------------------------------
def build_weighted_date_pool(start_date, end_date):
    dates = []
    weights = []
    n_days = (end_date - start_date).days
    for d in range(n_days + 1):
        day = start_date + timedelta(days=d)
        w = 1.0
        if day.weekday() >= 5:          # Sat/Sun
            w *= 1.6
        if day.month == 12:              # holiday season spike
            w *= 2.2
        # gentle growth trend over the window (later months slightly busier)
        w *= (1.0 + 0.3 * (d / max(n_days, 1)))
        dates.append(day)
        weights.append(w)
    return dates, weights


def sample_datetimes(date_pool, date_weights, n):
    chosen_days = random.choices(date_pool, weights=date_weights, k=n)
    out = []
    for day in chosen_days:
        # business hours skew: more orders 10am-9pm than middle of the night
        hour = int(random.triangular(0, 23, 15))
        minute = random.randint(0, 59)
        out.append(datetime(day.year, day.month, day.day, hour, minute, random.randint(0, 59)))
    return out


# ---------------------------------------------------------------
# ORDER GENERATION (weighted customer segments, weighted store regions, seasonal dates)
# ---------------------------------------------------------------
def compute_product_cum_weights(product_catalog):
    """product_catalog: list of (product_id, unit_price, unit_cost, category).
    Assigns each product a popularity rank via a STABLE hash of product_id (not
    Python's randomized hash()), so the same products are 'bestsellers' whether
    this runs in initial mode or in a daily batch fetched fresh from the DB.
    Returns cum_weights aligned to product_catalog's order, for use with
    random.choices(..., cum_weights=...) which is O(log n) per draw instead of
    O(n) - important since this gets called per line item across ~250K rows.
    """
    def stable_rank_key(pid):
        return int(hashlib.md5(str(pid).encode()).hexdigest(), 16)

    ranked_ids = sorted((pid for pid, *_ in product_catalog), key=stable_rank_key)
    rank_of = {pid: i + 1 for i, pid in enumerate(ranked_ids)}
    weights = [1.0 / (rank_of[pid] ** PRODUCT_ZIPF_S) for pid, *_ in product_catalog]
    return list(itertools.accumulate(weights))


def generate_orders_batch(n_orders, customer_pool, customer_weights,
                           store_pool, store_weights, date_pool, date_weights):
    chosen_customers = random.choices(customer_pool, weights=customer_weights, k=n_orders)
    chosen_stores = random.choices(store_pool, weights=store_weights, k=n_orders)
    chosen_dates = sample_datetimes(date_pool, date_weights, n_orders)

    orders = []
    for cust, store, order_dt in zip(chosen_customers, chosen_stores, chosen_dates):
        status = random.choices(["completed", "cancelled", "pending"], weights=[0.88, 0.08, 0.04])[0]
        currency = random.choices(CURRENCIES, weights=[0.7, 0.15, 0.1, 0.05])[0]
        orders.append((cust, store, order_dt, status, currency))
    return orders


def build_items_and_payments(order_ids, order_currencies, order_dates, product_catalog, product_cum_weights):
    """product_catalog: list of (product_id, unit_price, unit_cost, category)"""
    items = []
    payments = []
    for idx, (oid, currency, odate) in enumerate(zip(order_ids, order_currencies, order_dates)):
        # Electronics orders tend to have fewer, pricier items; Grocery/Apparel more items
        n_items = max(1, round(random.gauss(2.3, 1)))
        order_total = 0
        for _ in range(n_items):
            pid, price, cost, category = random.choices(product_catalog, cum_weights=product_cum_weights, k=1)[0]
            price = float(price)
            cost = float(cost)
            qty = random.randint(1, 4) if category != "Grocery" else random.randint(1, 8)
            price_at_sale = price
            if random.random() < 0.005:
                log_dirty("order_items", f"order_id~{oid}", "unit_price_at_sale", "negative line price")
                price_at_sale = round(-abs(price), 2)
            discount = round(random.choice([0, 0, 0, 5, 10, price_at_sale * 0.1]), 2) if price_at_sale > 0 else 0
            line_total = round((price_at_sale * qty) - discount, 2)
            order_total += line_total
            items.append((oid, pid, qty, price_at_sale, cost, discount, line_total))
        if random.random() < PAYMENT_RATE:
            pay_status = random.choices(["success", "failed", "refunded"], weights=[0.92, 0.05, 0.03])[0]
            payments.append((oid, random.choice(PAYMENT_METHODS), pay_status,
                              round(order_total, 2), currency, odate))
        # occasional missing customer_id on an order -> handled at insert time isn't feasible
        # with a NOT NULL FK column; documented as a known V1 scope limitation (see note below).
    return items, payments


def run_initial_load():
    conn = get_conn()
    cur = conn.cursor()

    print(f"Generating {N_CUSTOMERS} customers, {N_PRODUCTS} products, {N_STORES} stores...")
    customers = generate_customers(N_CUSTOMERS)
    inserted_c = psycopg2.extras.execute_values(
        cur,
        """INSERT INTO customers
           (first_name, last_name, email, phone, address, city, state, country, signup_date, is_active)
           VALUES %s RETURNING customer_id""",
        [c[:10] for c in customers],  # segment (11th field) is generator-only metadata, not a DB column
        fetch=True,
    )
    customer_ids = [r[0] for r in inserted_c]
    customer_segments = [c[10] for c in customers]

    products = generate_products(N_PRODUCTS)
    inserted_p = psycopg2.extras.execute_values(
        cur,
        """INSERT INTO products (product_name, category, sub_category, unit_price, unit_cost, is_active)
           VALUES %s RETURNING product_id""",
        products, fetch=True,
    )
    product_ids = [r[0] for r in inserted_p]
    product_categories = [p[1] for p in products]

    stores = generate_stores(N_STORES)
    inserted_s = psycopg2.extras.execute_values(
        cur,
        """INSERT INTO stores (store_name, store_type, store_size, region, country, opened_date)
           VALUES %s RETURNING store_id""",
        stores, fetch=True,
    )
    store_ids = [r[0] for r in inserted_s]
    store_sizes = [s[2] for s in stores]
    store_regions = [s[3] for s in stores]
    conn.commit()

    # weights for weighted sampling
    seg_weight_map = {s[0]: s[2] for s in CUSTOMER_SEGMENTS}
    customer_weights = [seg_weight_map[seg] for seg in customer_segments]
    store_weights = [
        REGION_PERFORMANCE_WEIGHT[r] * STORE_SIZE_CONFIG[sz]["order_multiplier"]
        for r, sz in zip(store_regions, store_sizes)
    ]
    product_catalog = list(zip(product_ids,
                                [p[3] for p in products],
                                [p[4] for p in products],
                                product_categories))
    product_cum_weights = compute_product_cum_weights(product_catalog)

    print(f"Generating {N_INITIAL_ORDERS} orders across a 12-month seasonal window...")
    end_date = (datetime.now() - timedelta(days=1)).date()
    start_date = end_date - timedelta(days=365)
    date_pool, date_weights = build_weighted_date_pool(start_date, end_date)

    batch_size = 5000
    total_items = 0
    total_payments = 0
    remaining = N_INITIAL_ORDERS
    while remaining > 0:
        n = min(batch_size, remaining)
        orders_batch = generate_orders_batch(
            n, customer_ids, customer_weights, store_ids, store_weights, date_pool, date_weights
        )
        inserted = psycopg2.extras.execute_values(
            cur,
            """INSERT INTO orders (customer_id, store_id, order_date, order_status, currency_code)
               VALUES %s RETURNING order_id, currency_code, order_date""",
            orders_batch, fetch=True,
        )
        order_ids = [r[0] for r in inserted]
        currencies = [r[1] for r in inserted]
        dates = [r[2] for r in inserted]

        items, payments = build_items_and_payments(order_ids, currencies, dates, product_catalog, product_cum_weights)
        psycopg2.extras.execute_values(
            cur,
            """INSERT INTO order_items
               (order_id, product_id, quantity, unit_price_at_sale, unit_cost_at_sale, discount_amount, line_total)
               VALUES %s""",
            items,
        )
        if payments:
            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO payments (order_id, payment_method, payment_status, amount, currency_code, payment_date)
                   VALUES %s""",
                payments,
            )
        conn.commit()
        total_items += len(items)
        total_payments += len(payments)
        remaining -= n
        print(f"  ...{N_INITIAL_ORDERS - remaining}/{N_INITIAL_ORDERS} orders done")

    print(f"Initial load complete: {N_INITIAL_ORDERS} orders, {total_items} order_items, {total_payments} payments")
    flush_dirty_manifest("initial")
    cur.close()
    conn.close()


def run_daily_batch(n_orders):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT customer_id, is_active FROM customers")
    cust_rows = cur.fetchall()
    active_customer_ids = [r[0] for r in cust_rows if r[1]]
    cur.execute("SELECT store_id, region, store_size FROM stores")
    store_rows = cur.fetchall()
    store_ids = [r[0] for r in store_rows]
    store_weights = [
        REGION_PERFORMANCE_WEIGHT[r[1]] * STORE_SIZE_CONFIG[r[2]]["order_multiplier"]
        for r in store_rows
    ]
    cur.execute("SELECT product_id, unit_price, unit_cost, category FROM products WHERE is_active = TRUE")
    product_catalog = cur.fetchall()
    product_cum_weights = compute_product_cum_weights(product_catalog)

    # daily batch: no strong segment info available post-load, so weight lightly by recent order count
    # (customers with more history get proportionally more new orders - simple recency-agnostic proxy)
    cur.execute("""SELECT customer_id, count(*) FROM orders GROUP BY customer_id""")
    hist_counts = dict(cur.fetchall())
    customer_weights = [max(1, hist_counts.get(cid, 1)) for cid in active_customer_ids]

    today = datetime.now().date()
    date_pool, date_weights = build_weighted_date_pool(today, today)  # today only, weighted by hour internally

    orders_batch = generate_orders_batch(
        n_orders, active_customer_ids, customer_weights, store_ids, store_weights, date_pool, date_weights
    )
    inserted = psycopg2.extras.execute_values(
        cur,
        """INSERT INTO orders (customer_id, store_id, order_date, order_status, currency_code)
           VALUES %s RETURNING order_id, currency_code, order_date""",
        orders_batch, fetch=True,
    )
    order_ids = [r[0] for r in inserted]
    currencies = [r[1] for r in inserted]
    dates = [r[2] for r in inserted]

    items, payments = build_items_and_payments(order_ids, currencies, dates, product_catalog, product_cum_weights)
    psycopg2.extras.execute_values(
        cur,
        """INSERT INTO order_items
           (order_id, product_id, quantity, unit_price_at_sale, unit_cost_at_sale, discount_amount, line_total)
           VALUES %s""",
        items,
    )
    if payments:
        psycopg2.extras.execute_values(
            cur,
            """INSERT INTO payments (order_id, payment_method, payment_status, amount, currency_code, payment_date)
               VALUES %s""",
            payments,
        )

    # SCD1-style updates
    n_customer_updates = max(1, n_orders // 200)
    n_product_updates = max(1, n_orders // 500)

    sample_customers = random.sample(active_customer_ids, min(n_customer_updates, len(active_customer_ids)))
    for cid in sample_customers:
        cur.execute(
            "UPDATE customers SET city=%s, state=%s, updated_at=now() WHERE customer_id=%s",
            (fake.city()[:100], random.choice(STATE_VARIANTS), cid),
        )

    cur.execute("SELECT product_id FROM products")
    product_ids = [r[0] for r in cur.fetchall()]
    sample_products = random.sample(product_ids, min(n_product_updates, len(product_ids)))
    for pid in sample_products:
        cur.execute(
            "UPDATE products SET unit_cost = ROUND(unit_cost * %s, 2), updated_at = now() WHERE product_id = %s",
            (random.uniform(0.95, 1.1), pid),
        )

    conn.commit()
    print(f"Daily batch complete: {len(order_ids)} orders, {len(items)} order_items, "
          f"{len(payments)} payments, {len(sample_customers)} customer updates, "
          f"{len(sample_products)} product cost updates")
    flush_dirty_manifest("daily")
    cur.close()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["initial", "daily"], required=True)
    parser.add_argument("--orders", type=int, default=2500)
    args = parser.parse_args()

    if args.mode == "initial":
        run_initial_load()
    else:
        run_daily_batch(args.orders)
