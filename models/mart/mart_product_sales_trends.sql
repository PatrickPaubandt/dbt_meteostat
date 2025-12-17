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

    CASE
        WHEN prev_month_revenue IS NULL THEN NULL
        WHEN prev_month_revenue = 0 THEN NULL
        ELSE
            (total_revenue - prev_month_revenue) * 100.0
            / prev_month_revenue
    END AS revenue_change_pct_mom,

    CASE
        WHEN prev_month_revenue IS NULL THEN 'no sales prev month'
        WHEN prev_month_revenue = 0 THEN 'no sales prev month'
        ELSE 'ok'
    END AS mom_status

FROM mom_calc
ORDER BY product_name, order_year, order_month
