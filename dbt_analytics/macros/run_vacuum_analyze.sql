{% macro run_vacuum_analyze() %}
  {%- if flags.WHICH in ["build", "run"] -%}

    {% set query %}
      VACUUM ANALYZE
    {% endset %}

    {% do run_query(query) %}

  {% endif %}
{% endmacro %}
