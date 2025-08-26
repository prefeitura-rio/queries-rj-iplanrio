{% macro generate_hash(campo1, campo2) %}
  {% set config = var('phone_validation') %}
  {% if config.hash_algorithm == 'sha256' %}
    sha256(concat(cast({{ campo1 }} as string), coalesce(to_json_string({{ campo2 }}), 'null')))
  {% else %}
    farm_fingerprint(concat(cast({{ campo1 }} as string), coalesce(to_json_string({{ campo2 }}), 'null')))
  {% endif %}
{% endmacro %}