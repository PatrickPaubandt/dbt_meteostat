with order_details_data as (
    select *
    from {{ source('northwind', 'order_details') }}
)

select
    order_id,
    product_id,
    unit_price::numeric as unit_price,
    quantity::int as quantity,
    discount::numeric as discount
from order_details_data