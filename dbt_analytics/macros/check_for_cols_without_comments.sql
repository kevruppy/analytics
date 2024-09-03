{% macro check_for_comments_on_cols() %}
	{% set query %}
    SELECT
        1
    FROM
        INFORMATION_SCHEMA.COLUMNS
    WHERE
        TABLE_SCHEMA NOT IN ({{ var('ignored_schemas') }})
    AND
        COLUMN_COMMENT IS NULL UNION ALL SELECT 1
  {% endset %}

  {% set results = run_query(query) %}

  {% if results|length > 0 %}
    {% do exceptions.warn("Some columns are not commented!") %}
  {% endif %}
{% endmacro %}
