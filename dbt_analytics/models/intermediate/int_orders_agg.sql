WITH CTE_ORDERS AS (
    SELECT
        DATE_TRUNC('MONTH', CREATION_DATE) AS CREATION_MONTH
        ,PRODUCT_NAME
        ,CASE WHEN CONFIRMATIO
    FROM
        {{ ref('int_orders_status_dates') }}
    GROUP BY ALL
)