{% test value_proportion(model, expression, threshold) %}
-- TODO: CAN WE LOG VALUE TRIGGERING ERROR (ABOVE THRESHOLD)
WITH CTE_CHECK AS
(
SELECT
    (COUNT_IF( {{ expression }} ) / COUNT(*)) > {{ threshold }} AS _CHECK
FROM
    {{ model }}
)
SELECT
    COLUMNS(*)
FROM
    CTE_CHECK
WHERE
    _CHECK = FALSE
{% endtest %}
