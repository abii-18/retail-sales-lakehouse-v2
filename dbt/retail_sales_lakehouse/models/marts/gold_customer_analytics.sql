{{ config(
    materialized='table'
) }}

select
    c.customer_key,
    c.customer_id,
    c.first_name,
    c.last_name,
    c.city,
    c.state,
    c.country,

    count(distinct f.order_id) as total_orders,
    sum(f.quantity) as total_items_purchased,
    sum(f.line_total) as total_spent,
    sum(f.profit) as total_profit,
    avg(f.line_total) as average_line_item_value,
    max(f.date_key) as last_order_date

from {{ ref('fact_sales') }} f

join {{ ref('dim_customer') }} c
    on f.customer_key = c.customer_key

group by
    c.customer_key,
    c.customer_id,
    c.first_name,
    c.last_name,
    c.city,
    c.state,
    c.country