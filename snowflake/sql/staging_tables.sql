USE DATABASE RETAIL_LAKEHOUSE;
USE SCHEMA STAGING;

CREATE OR REPLACE TABLE CUSTOMERS (
    customer_id INTEGER,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    address STRING,
    city STRING,
    state STRING,
    country STRING,
    signup_date DATE,
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    ingestion_timestamp TIMESTAMP_NTZ,
    batch_date DATE,
    source_system STRING,
    source_file STRING
);

CREATE OR REPLACE TABLE PRODUCTS (
    product_id INTEGER,
    product_name STRING,
    category STRING,
    sub_category STRING,
    unit_price DOUBLE,
    unit_cost DOUBLE,
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    ingestion_timestamp TIMESTAMP_NTZ,
    batch_date DATE,
    source_system STRING,
    source_file STRING
);

CREATE OR REPLACE TABLE STORES (
    store_id INTEGER,
    store_name STRING,
    store_type STRING,
    store_size STRING,
    region STRING,
    country STRING,
    opened_date DATE,
    created_at TIMESTAMP_NTZ,
    ingestion_timestamp TIMESTAMP_NTZ,
    batch_date DATE,
    source_system STRING,
    source_file STRING
);

CREATE OR REPLACE TABLE ORDERS (
    order_id INTEGER,
    customer_id INTEGER,
    store_id INTEGER,
    order_date TIMESTAMP_NTZ,
    order_status STRING,
    currency_code STRING,
    created_at TIMESTAMP_NTZ,
    ingestion_timestamp TIMESTAMP_NTZ,
    batch_date DATE,
    source_system STRING,
    source_file STRING,
    exchange_rate DOUBLE
);

CREATE OR REPLACE TABLE ORDER_ITEMS (
    order_item_id INTEGER,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    unit_price_at_sale DOUBLE,
    unit_cost_at_sale DOUBLE,
    discount_amount DOUBLE,
    line_total DOUBLE,
    created_at TIMESTAMP_NTZ,
    ingestion_timestamp TIMESTAMP_NTZ,
    batch_date DATE,
    source_system STRING,
    source_file STRING
);

CREATE OR REPLACE TABLE PAYMENTS (
    payment_id INTEGER,
    order_id INTEGER,
    payment_method STRING,
    payment_status STRING,
    amount DOUBLE,
    currency_code STRING,
    payment_date TIMESTAMP_NTZ,
    created_at TIMESTAMP_NTZ,
    ingestion_timestamp TIMESTAMP_NTZ,
    batch_date DATE,
    source_system STRING,
    source_file STRING
);

LIST @SILVER_INTERNAL_STAGE;