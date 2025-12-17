with category_data as (
    select *
    from {{ source('northwind', 'categories') }}
)

select
    category_id,
    category_name,
from category_data