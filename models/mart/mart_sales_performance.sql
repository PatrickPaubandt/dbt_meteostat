WITH sales_performance AS (
    SELECT * 
    FROM {{ ref('prep_sales') }}
),
aggregation_sales AS (
    SELECT
        order_year,
        order_month,
        category_name,
        SUM(revenue) AS total_revenue,
        COUNT(DISTINCT order_id) AS total_orders,
        AVG(revenue) as avg_revenue_per_category,
        SUM(revenue) * 1.0 / NULLIF(COUNT(DISTINCT order_id), 0) AS avg_revenue_per_order

    FROM sales_performance
    GROUP BY order_year, order_month, category_name
)
SELECT *
FROM aggregation_sales
ORDER BY order_year, order_month, category_name