{% macro zero_copy_clone_db (tgt_db, src_db) %}
  {% if target.name == 'prod' %}

    {% set qry %}
      CREATE OR REPLACE DATABASE {{ tgt_db }} FROM {{ src_db }}
    {% endset %}

    {% do run_query(qry) %}
    {{ print("\n### SUCCESS: Cloned database '" ~ src_db ~ "' to '" ~ tgt_db ~ "'  ###\n") }}

  {% else %}
    {{ print("\n### SKIPPING: Macro is only executed on prod ###\n") }}

  {% endif %}
{% endmacro %}
