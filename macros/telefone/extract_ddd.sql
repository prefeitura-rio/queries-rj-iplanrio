{% macro extract_ddd(numero_completo) %}
  {% set config = var('phone_validation') %}
  case
    when starts_with({{ numero_completo }}, '55') 
    then substr({{ numero_completo }}, 3, 2)
    else null
  end
{% endmacro %}