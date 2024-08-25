{%- materialization delete_and_re_insert, adapter='default' -%}

  {%- set target_relation = api.Relation.create(
        identifier=this.identifier, schema=this.schema, database=this.database,
        type='table') -%}

  -- build model
  {% call statement('main') -%}
    {{ create_view_as(target_relation, sql) }}
  {%- endcall %}
  

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization -%}

