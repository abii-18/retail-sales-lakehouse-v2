{{ config(
    materialized='table'
) }}

select
    date_key,
    count(distinct order_id) as total_orders,
    sum(quantity) as total_items_sold,
    sum(line_total) as total_revenue,
    sum(profit) as total_profit,
    avg(line_total) as average_line_item_value

from {{ ref('fact_sales') }}

group by date_key
order by date_key