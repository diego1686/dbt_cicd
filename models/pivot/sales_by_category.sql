WITH sales_data AS (
    SELECT
        product,
        category,
        {{ mult_1000('num_sold') }} as num_sold,
        updated_at
    FROM {{ ref('sales') }}
)

SELECT
    category,
    SUM(num_sold) AS total_sales,
    current_datetime() as updated_at
FROM
    sales_data
GROUP BY
    category