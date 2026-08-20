from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2

DB_CONFIG = {
    "host": "postgres",
    "port": "5432",
    "dbname": "retail_lakehouse",
    "user": "retail_user",
    "password": "retail_pass",
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def get_last_watermark(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT last_watermark
            FROM etl_watermark
            WHERE pipeline_name = 'orders_pipeline';
            """
        )

        return cur.fetchone()[0]



# extracting orders based on last waermark that was successful
def extract_orders(conn, last_watermark):
    query = """
        SELECT *
        FROM orders
        WHERE order_date > %s
        ORDER BY order_date;
    """

    df = pd.read_sql(query, conn, params=(last_watermark,))
    return df

def extract_customers(conn):
    query = """
        SELECT *
        FROM customers
        ORDER BY customer_id;
    """

    df = pd.read_sql(query, conn)
    return df

def extract_products(conn):
    query = """
        SELECT *
        FROM products
        ORDER BY product_id;
    """

    df = pd.read_sql(query, conn)
    return df

def extract_stores(conn):
    query = """
        SELECT *
        FROM stores
        ORDER BY store_id;
    """

    df = pd.read_sql(query, conn)
    return df

def extract_order_items(conn):
    query = """
        SELECT *
        FROM order_items
        ORDER BY order_item_id;
    """

    df = pd.read_sql(query, conn)
    return df

def extract_payments(conn):
    query = """
        SELECT *
        FROM payments
        ORDER BY payment_id;
    """

    df = pd.read_sql(query, conn)
    return df


# saving to staging(l1) table
def save_to_staging(df, table_name):
    batch_date = datetime.now().strftime("%Y-%m-%d")

    output_dir = Path("staging") / table_name / f"batch_date={batch_date}"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"{table_name}.csv"

    df.to_csv(output_file, index=False)

    print(f"{table_name} : {len(df)} rows extracted")
    print(f"Saved to {output_file}")

    return output_file


#updating only to get oly incremental data
def update_watermark(conn, watermark):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE etl_watermark
            SET
                last_watermark = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE pipeline_name = 'orders_pipeline';
            """,
            (watermark,),
        )

    conn.commit()

def main():
    conn = get_connection()

    try:
        # Incremental Orders
        last_watermark = get_last_watermark(conn)

        orders_df = extract_orders(conn, last_watermark)

        if not orders_df.empty:
            new_watermark = orders_df["order_date"].max()
            save_to_staging(orders_df, "orders")

            print(f"New watermark: {new_watermark}")

            update_watermark(conn, new_watermark)

            print("Orders watermark updated successfully.")

        else:
            print("No new orders found.")
        # Full Customers
        customers_df = extract_customers(conn)
        save_to_staging(customers_df, "customers")

        # Full Products
        products_df = extract_products(conn)
        save_to_staging(products_df, "products")

        # Full Stores
        stores_df = extract_stores(conn)
        save_to_staging(stores_df, "stores")

        # Full Order Items
        order_items_df = extract_order_items(conn)
        save_to_staging(order_items_df, "order_items")

        # Full Payments
        payments_df = extract_payments(conn)
        save_to_staging(payments_df, "payments")

        print("\nAll tables extracted successfully.")

    finally:
        conn.close()


if __name__ == "__main__":
    main() 
