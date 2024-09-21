{% test value_proportion(model, expression, threshold) %}
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
    TRUE
    AND
    _CHECK = FALSE
{% endtest %}
