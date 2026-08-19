select
    product_id,
    product_name,
    category,
    sub_category,
    unit_price,
    unit_cost,
    is_active,
    created_at,
    updated_at,
    ingestion_timestamp,
    batch_date,
    source_system,
    source_file

from {{ source('staging', 'PRODUCTS') }}