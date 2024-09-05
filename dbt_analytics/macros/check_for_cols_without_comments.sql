{% macro check_for_cols_without_comments() %}
  {%- if flags.WHICH in ["build", "run"] -%}
    {{ print("\n### Check if all columns are commented... ###\n") }}

    {% set query %}
      SELECT
        1
      FROM
        INFORMATION_SCHEMA.COLUMNS
      WHERE
        TABLE_SCHEMA NOT IN ({{ var('ignored_schemas') }})
      AND
        LEFT(COLUMN_NAME,4) != 'dbt_' -- DO NOT CHECK COLS SET UP BY DBT
      AND
        COLUMN_COMMENT IS NULL
    {% endset %}

    {% set results = run_query(query) %}

    {% if results|length > 0 %}
      {% do exceptions.raise_compiler_error("At least one column is not commented!") %}
    {% else %}
      {{ print("\n### All columns are commented ###\n") }}
    {% endif %}
  {% endif %}
{% endmacro %}
