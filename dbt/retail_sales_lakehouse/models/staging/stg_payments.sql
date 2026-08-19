select
    payment_id,
    order_id,
    payment_method,
    payment_status,
    amount,
    currency_code,
    payment_date,
    created_at,
    ingestion_timestamp,
    batch_date,
    source_system,
    source_file

from {{ source('staging', 'PAYMENTS') }}