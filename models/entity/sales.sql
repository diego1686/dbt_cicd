{{
  config(
    materialized='incremental',
    unique_key=['product', 'category', 'source']
  )
}}

WITH web_sales AS (
    SELECT
        product,
        price,
        num_sold,
        category,
        'web' AS source,
        updated_at
    FROM {{ ref('web_sales') }}
),

retail_sales AS (
    SELECT
        product,
        price,
        num_sold,
        category,
        'retail' AS source,
        updated_at
    FROM {{ ref('retail_sales') }}
),

combined_sales AS (
    SELECT * FROM web_sales
    UNION ALL
    SELECT * FROM retail_sales
)

SELECT
    product,
    category,
    source,
    SUM(num_sold) AS num_sold,
    current_datetime() as updated_at
FROM
    combined_sales

{% if is_incremental() %}
WHERE
  updated_at > (select max(updated_at) from {{ this }})
{% endif %}

GROUP BY
    product,
    category,
    source

