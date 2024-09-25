# macro desc

{% docs dsc_check_for_cols_without_comments %}
Checks if there are any undocumented columns
{% enddocs %}

{% docs dsc_generate_schema_name %}
Determines the name of the schema that a model should be built in
{% enddocs %}

{% docs dsc_run_vacuum_analyze %}
Runs `VACUUM ANALYZE`
{% enddocs %}

{% docs dsc_zero_copy_clone_db %}
Clones a database (re-creation)
{% enddocs %}

# arg desc

{% docs dsc_custom_schema_name %}
The configured value of `schema` in the specified node, or `none` if a value is not supplied
{% enddocs %}

{% docs dsc_node %}
The `node` that is currently being processed by dbt
{% enddocs %}

{% docs dsc_tgt_db %}
Name of the database to be created as a clone
{% enddocs %}

{% docs dsc_src_db %}
Name of the database to be cloned
{% enddocs %}
