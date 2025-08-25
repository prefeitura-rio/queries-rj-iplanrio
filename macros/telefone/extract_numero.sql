{% macro extract_numero(numero_completo) %}
  {% set config = var('phone_validation') %}
  case
    when starts_with({{ numero_completo }}, '55')
    then substr({{ numero_completo }}, 5)
    else {{ numero_completo }}
  end
{% endmacro %}