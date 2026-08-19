select
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    address,
    city,
    state,
    country,
    signup_date,
    is_active,
    created_at,
    updated_at,
    ingestion_timestamp,
    batch_date,
    source_system,
    source_file

from {{ source('staging', 'CUSTOMERS') }}