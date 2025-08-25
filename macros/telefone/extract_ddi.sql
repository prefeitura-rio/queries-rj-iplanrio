{% macro extract_ddi(numero_completo) %}
  {% set config = var('phone_validation') %}
  case
    when starts_with({{ numero_completo }}, '55') then '55'
    when starts_with({{ numero_completo }}, '1') then '1'
    when starts_with({{ numero_completo }}, '54') then '54'
    -- Adicionar outros DDIs conforme necessário
    else substr({{ numero_completo }}, 1, 2)
  end
{% endmacro %}