import json
from pathlib import Path

import pandas as pd


REQUIRED_ORDER_COLUMNS = [
    "order_id", "customer_id", "store_id", "order_date", "order_status", "currency_code"
]


def validate_table(table_name, required_columns=None, skip_if_missing=False):
    batch_dirs = sorted(Path(f"staging/{table_name}").glob("batch_date=*"), reverse=True)

    if not batch_dirs:
        if skip_if_missing:
            print(f"No {table_name} batch found. Skipping validation.")
            return

        raise FileNotFoundError(f"No {table_name} batch found.")

    table_file = batch_dirs[0] / f"{table_name}.csv"

    if not table_file.exists():
        raise FileNotFoundError(f"{table_name}.csv not found.")

    if table_file.stat().st_size == 0:
        raise ValueError(f"{table_name}.csv is empty.")

    df = pd.read_csv(table_file)

    if df.empty:
        raise ValueError(f"{table_name}.csv contains no records.")

    if required_columns:
        missing_columns = [column for column in required_columns if column not in df.columns]

        if missing_columns:
            raise ValueError(f"Missing columns: {missing_columns}")

    print(f"{table_name} validation passed.")


def validate_orders():
    validate_table("orders", required_columns=REQUIRED_ORDER_COLUMNS, skip_if_missing=True)


def validate_customers():
    validate_table("customers")


def validate_products():
    validate_table("products")


def validate_stores():
    validate_table("stores")


def validate_order_items():
    validate_table("order_items")


def validate_payments():
    validate_table("payments")


def validate_currency():
    currency_dir = Path("staging") / "currency"

    required_files = ["fx_rates.csv", "latest_rates.json", "fx_status.json"]

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
