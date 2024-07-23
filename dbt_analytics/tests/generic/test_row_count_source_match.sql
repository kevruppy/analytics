{% test row_count_source_match(model, source_name, table_name) %}
WITH ROW_COUNTS AS (
    SELECT COUNT(*) AS CNT FROM {{ model }}
    UNION
    SELECT COUNT(*) AS CNT FROM {{ source(source_name, table_name) }}
),
CTE_CHECK AS (
    SELECT 
        CASE 
            WHEN COUNT(*) OVER () > 1 THEN 'FAILED' 
            ELSE 'PASSED'
        END AS TEST_RESULT
    FROM ROW_COUNTS
)
SELECT * FROM CTE_CHECK WHERE TEST_RESULT = 'FAILED'
{% endtest %}
