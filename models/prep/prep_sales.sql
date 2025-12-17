with order_data as (
    Select * from {{ ref('staging_orders') }}
),
order_details_data as (
    Select * from {{ ref('staging_order_details') }}
),
products_data as (
    Select * from {{ ref('staging_products') }}
),
category_data as (
    Select * from {{ ref('staging_categories') }}
),
combined_data as (
    Select o.order_id,
           o.customer_id,
           o.order_date,
           extract(year from o.order_date) as order_year,
           extract(month from o.order_date) as order_month,
           extract(day from o.order_date) as order_day
           od.unit_price,
           od.quantity,
           od.discount,
           (od.unit_price * od.quantity) * (1-od.discount) as revenue,
           p.product_id,
           p.product_name,
           p.category_id,
           c.category_name
    FROM order_data o
    JOIN order_details_data od USING (order_id)
    JOIN products_data p USING (product_id)
    LEFT JOIN category_data c USING (category_id)
)
Select * from combined_data
