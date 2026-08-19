select
    order_id,
    customer_id,
    store_id,
    order_date,
    order_status,
    currency_code,
    exchange_rate,
    created_at,
    ingestion_timestamp,
    batch_date,
    source_system,
    source_file

from {{ source('staging', 'ORDERS') }}