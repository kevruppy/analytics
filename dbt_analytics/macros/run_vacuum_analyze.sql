{% macro run_vacuum_analyze() %}
  {%- if flags.WHICH in ["build", "run"] -%}

    {% set qry %}
      VACUUM ANALYZE
    {% endset %}

    {% do run_query(qry) %}

  {% endif %}
{% endmacro %}
