select
    store_id,
    store_name,
    store_type,
    store_size,
    region,
    country,
    opened_date,
    created_at,
    ingestion_timestamp,
    batch_date,
    source_system,
    source_file

from {{ source('staging', 'STORES') }}