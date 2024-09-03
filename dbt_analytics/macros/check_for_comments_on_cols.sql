{% macro check_for_comments_on_cols() %}
	{% set query %}
    SELECT
        COUNT(*)+1 = 0
    FROM
        INFORMATION_SCHEMA.COLUMNS
    WHERE
        TABLE_SCHEMA NOT IN ({{ var('ignored_schemas') }})
    AND
        COLUMN_COMMENT IS NULL
  {% endset %}

  {% set results = run_query(query) %}

  {% if results.rows[0][0] %}
    {% do log(level='warning', message='Some columns are not commented!') %}
  {% endif %}
{% endmacro %}
