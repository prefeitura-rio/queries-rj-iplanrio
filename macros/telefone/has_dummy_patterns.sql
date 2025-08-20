{% macro has_dummy_patterns(numero_completo) %}
  {% set config = var('phone_validation') %}
  (
    -- Padrões conhecidos como falsos
    {% for pattern in config.dummy_patterns %}
    strpos({{ numero_completo }}, '{{ pattern }}') > 0
    {% if not loop.last %} or {% endif %}
    {% endfor %}
  )
{% endmacro %}