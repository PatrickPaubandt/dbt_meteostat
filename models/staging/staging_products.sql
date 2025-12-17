with products_data as (
    select *
    from {{ source('northwind', 'products') }}
)

select
    product_id,
    product_name,
    supplier_id,
    category_id,
    unit_price::numeric as unit_price,
from products_data