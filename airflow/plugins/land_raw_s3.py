import os
from datetime import datetime
from pathlib import Path

import boto3

BUCKET_NAME = os.getenv("RAW_BUCKET_NAME")
print(BUCKET_NAME)

s3_client = boto3.client("s3")


def upload_to_s3(local_file, s3_key):
    local_file = Path(local_file)

    if not local_file.exists():
        raise FileNotFoundError(f"{local_file} not found.")

    s3_client.upload_file(Filename=str(local_file), Bucket=BUCKET_NAME, Key=s3_key)

    print(f"Uploaded {local_file}")
    print(f"s3://{BUCKET_NAME}/{s3_key}")


def upload_directory(local_directory, s3_prefix):
    local_directory = Path(local_directory)

    if not local_directory.exists():
        raise FileNotFoundError(f"{local_directory} not found.")

    for file in local_directory.iterdir():
        if file.is_file():
            upload_to_s3(local_file=file, s3_key=f"{s3_prefix}/{file.name}")


def land_raw():
    batch_date = datetime.today().strftime("%Y-%m-%d")

    orders_file = Path("staging") / "orders" / f"batch_date={batch_date}" / "orders.csv"
    if orders_file.exists():
        upload_to_s3(
            local_file=orders_file,
            s3_key=f"raw/orders/batch_date={batch_date}/orders.csv",
        )
    else:
        print("No new orders found. Skipping orders upload.")

    customers_file = Path("staging") / "customers" / f"batch_date={batch_date}" / "customers.csv"
    upload_to_s3(
        local_file=customers_file,
        s3_key=f"raw/customers/batch_date={batch_date}/customers.csv",
    )

    products_file = Path("staging") / "products" / f"batch_date={batch_date}" / "products.csv"
    upload_to_s3(
        local_file=products_file,
        s3_key=f"raw/products/batch_date={batch_date}/products.csv",
    )

    stores_file = Path("staging") / "stores" / f"batch_date={batch_date}" / "stores.csv"
    upload_to_s3(
        local_file=stores_file,
        s3_key=f"raw/stores/batch_date={batch_date}/stores.csv",
    )

    order_items_file = Path("staging") / "order_items" / f"batch_date={batch_date}" / "order_items.csv"
    upload_to_s3(
        local_file=order_items_file,
        s3_key=f"raw/order_items/batch_date={batch_date}/order_items.csv",
    )

    payments_file = Path("staging") / "payments" / f"batch_date={batch_date}" / "payments.csv"
    upload_to_s3(
        local_file=payments_file,
        s3_key=f"raw/payments/batch_date={batch_date}/payments.csv",
    )

    currency_directory = Path("staging") / "currency"
    upload_directory(
        local_directory=currency_directory,
        s3_prefix=f"raw/currency/batch_date={batch_date}",
    )

    print("\nRaw landing completed successfully.")


if __name__ == "__main__":
    land_raw()
