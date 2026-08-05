from pathlib import Path
import json

import pandas as pd


REQUIRED_ORDER_COLUMNS = [
    "order_id",
    "customer_id",
    "store_id",
    "order_date",
    "order_status",
    "currency_code"
]


def validate_orders():

    batch_dirs = sorted(Path("staging/orders").glob("batch_date=*"), reverse=True)

    if not batch_dirs:
        print("No incremental orders found. Skipping orders validation.")
        return

    orders_file = batch_dirs[0] / "orders.csv"

    if not orders_file.exists():
        print("orders.csv not found. Skipping orders validation.")
        return

    if orders_file.stat().st_size == 0:
        print("orders.csv is empty. Skipping orders validation.")
        return

    df = pd.read_csv(orders_file)

    missing_columns = [column for column in REQUIRED_ORDER_COLUMNS if column not in df.columns]

    if missing_columns:
        raise ValueError(
            f"Missing columns: {missing_columns}"
        )

    print("Orders validation passed.")

def validate_customers():

    batch_dirs = sorted(Path("staging/customers").glob("batch_date=*"), reverse=True)

    if not batch_dirs:
        raise FileNotFoundError("No customers batch found.")

    customers_file = batch_dirs[0] / "customers.csv"

    if not customers_file.exists():
        raise FileNotFoundError("customers.csv not found.")

    if customers_file.stat().st_size == 0:
        raise ValueError("customers.csv is empty.")

    df = pd.read_csv(customers_file)

    if df.empty:
        raise ValueError("customers.csv contains no records.")

    print("Customers validation passed.")


def validate_products():

    batch_dirs = sorted(Path("staging/products").glob("batch_date=*"), reverse=True)

    if not batch_dirs:
        raise FileNotFoundError("No products batch found.")

    products_file = batch_dirs[0] / "products.csv"

    if not products_file.exists():
        raise FileNotFoundError("products.csv not found.")

    if products_file.stat().st_size == 0:
        raise ValueError("products.csv is empty.")

    df = pd.read_csv(products_file)

    if df.empty:
        raise ValueError("products.csv contains no records.")

    print("Products validation passed.")

def validate_stores():

    batch_dirs = sorted(Path("staging/stores").glob("batch_date=*"), reverse=True)

    if not batch_dirs:
        raise FileNotFoundError("No stores batch found.")

    stores_file = batch_dirs[0] / "stores.csv"

    if not stores_file.exists():
        raise FileNotFoundError("stores.csv not found.")

    if stores_file.stat().st_size == 0:
        raise ValueError("stores.csv is empty.")

    df = pd.read_csv(stores_file)

    if df.empty:
        raise ValueError("stores.csv contains no records.")

    print("Stores validation passed.")

def validate_order_items():

    batch_dirs = sorted(Path("staging/order_items").glob("batch_date=*"), reverse=True)

    if not batch_dirs:
        raise FileNotFoundError("No order_items batch found.")

    order_items_file = batch_dirs[0] / "order_items.csv"

    if not order_items_file.exists():
        raise FileNotFoundError("order_items.csv not found.")

    if order_items_file.stat().st_size == 0:
        raise ValueError("order_items.csv is empty.")

    df = pd.read_csv(order_items_file)

    if df.empty:
        raise ValueError("order_items.csv contains no records.")

    print("Order Items validation passed.")


def validate_payments():

    batch_dirs = sorted(Path("staging/payments").glob("batch_date=*"), reverse=True)

    if not batch_dirs:
        raise FileNotFoundError("No payments batch found.")

    payments_file = batch_dirs[0] / "payments.csv"

    if not payments_file.exists():
        raise FileNotFoundError("payments.csv not found.")

    if payments_file.stat().st_size == 0:
        raise ValueError("payments.csv is empty.")

    df = pd.read_csv(payments_file)

    if df.empty:
        raise ValueError("payments.csv contains no records.")

    print("Payments validation passed.")



def validate_currency():

    currency_dir = Path("staging") / "currency"

    required_files = [
        "fx_rates.csv",
        "latest_rates.json",
        "fx_status.json"
    ]

    for filename in required_files:

        file_path = currency_dir / filename

        if not file_path.exists():
            raise FileNotFoundError(f"{filename} not found.")

        if file_path.stat().st_size == 0:
            raise ValueError(f"{filename} is empty.")

        if file_path.suffix == ".csv":
            pd.read_csv(file_path)

        elif file_path.suffix == ".json":
            with open(file_path, "r") as file:
                json.load(file)

    print("Currency validation passed.")

def main():

    validate_orders()

    validate_customers()

    validate_products()

    validate_stores()

    validate_order_items()

    validate_payments()

    validate_currency()

    print("\nRaw validation completed successfully.")


if __name__ == "__main__":
    main()
