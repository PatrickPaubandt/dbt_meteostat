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
    product_id,
    product_name,
    order_year,
    order_month,
    total_revenue,

    CASE
        WHEN prev_month_revenue IS NULL OR prev_month_revenue = 0 THEN NULL
        ELSE ROUND((total_revenue - prev_month_revenue) * 100.0 / prev_month_revenue, 2)
    END AS revenue_change_pct_mom,

    CASE
        WHEN prev_month_revenue IS NULL OR prev_month_revenue = 0 THEN 'no sales prev month'
        ELSE 'ok'
    END AS mom_status

FROM mom_calc
ORDER BY product_name, order_year, order_month

