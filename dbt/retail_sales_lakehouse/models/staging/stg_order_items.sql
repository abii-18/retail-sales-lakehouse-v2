select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price_at_sale,
    unit_cost_at_sale,
    discount_amount,
    line_total,
    created_at,
    ingestion_timestamp,
    batch_date,
    source_system,
    source_file

from {{ source('staging', 'ORDER_ITEMS') }}