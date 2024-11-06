{% test category_sales_totals(model, sales_model) %}

WITH sales_aggregated AS (
    SELECT
        category,
        SUM(num_sold) AS calculated_total_sales
    FROM {{ sales_model }}
    GROUP BY category
),

mismatched_totals AS (
    SELECT
        sbc.category,
        sbc.total_sales,
        sa.calculated_total_sales
    FROM {{model}} sbc
    LEFT JOIN sales_aggregated sa
    ON sbc.category = sa.category
    WHERE sbc.total_sales != sa.calculated_total_sales
)

SELECT * FROM mismatched_totals

{% endtest %}