{{ config(
    materialized='table'
) }}

select
    p.product_key,
    p.product_id,
    p.product_name,
    p.category,
    p.sub_category,

    count(distinct f.order_id) as total_orders,
    sum(f.quantity) as units_sold,
    sum(f.line_total) as total_revenue,
    sum(f.profit) as total_profit,
    avg(f.line_total) as average_sale_value

from {{ ref('fact_sales') }} f

join {{ ref('dim_product') }} p
    on f.product_key = p.product_key

group by
    p.product_key,
    p.product_id,
    p.product_name,
    p.category,
    p.sub_category

order by
    total_revenue desc