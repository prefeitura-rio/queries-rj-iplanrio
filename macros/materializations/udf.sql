{% materialization udf, adapter='bigquery' %}

  {%- set target_relation = this %}
  {%- set existing_relation = load_relation(this) %}

  {%- set parameter_list = config.get('parameter_list', '') %}
  {%- set returns = config.get('returns', 'STRING') %}
  {%- set language = config.get('language', 'sql') %}
  {%- set library = config.get('library', []) %}
  {%- set description = config.get('description', '') %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% call statement('main') %}
    CREATE OR REPLACE FUNCTION {{ target_relation }}({{ parameter_list }})
    RETURNS {{ returns }}
    {% if language == 'js' %}
    LANGUAGE js
    {% if library %}
    OPTIONS(library={{ library | tojson }})
    {% endif %}
    AS '''
    {{ sql }}
    ''';
    {% else %}
    AS (
    {{ sql }}
    );
    {% endif %}
  {% endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}