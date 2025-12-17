WITH sales_performance AS (
    SELECT *
    FROM {{ ref('prep_sales') }}
),

monthly_product AS (
    SELECT
        order_year,
        order_month,
        product_id,
        product_name,
        SUM(revenue) AS total_revenue
    FROM sales_performance
    GROUP BY
        order_year,
        order_month,
        product_id,
        product_name
),

mom_calc AS (
    SELECT
        order_year,
        order_month,
        product_id,
        product_name,
        total_revenue,

        LAG(total_revenue) OVER (
            PARTITION BY product_id
            ORDER BY order_year, order_month
        ) AS prev_month_revenue
    FROM monthly_product
)

SELECT
    order_year,
    order_month,
    product_id,
    product_name,
    total_revenue,

    (total_revenue - prev_month_revenue) * 100.0
      / NULLIF(prev_month_revenue, 0) AS revenue_change_pct_mom

FROM mom_calc
ORDER BY order_year, order_month, product_name
