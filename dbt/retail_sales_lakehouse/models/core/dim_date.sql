{{ config(
    materialized = 'table'
) }}

select distinct
    cast(order_date as date) as date_key,
    cast(order_date as date) as full_date,
    year(order_date) as year,
    quarter(order_date) as quarter,
    month(order_date) as month,
    monthname(order_date) as month_name,
    day(order_date) as day,
    dayofweek(order_date) as day_of_week,
    dayname(order_date) as day_name
from {{ ref('stg_orders') }}