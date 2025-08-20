{% macro is_format_valid(numero_completo) %}
  {% set config = var('phone_validation') %}
  {% set brasil = config.countries.brasil %}
  (
    -- Validação para números brasileiros
    (
      starts_with({{ numero_completo }}, '{{ brasil.ddi }}')
      and length({{ numero_completo }}) in (12, 13, 14)  -- 55 + DDD + 8/9 dígitos (12=landline, 13-14=mobile)
      and cast({{ extract_ddd(numero_completo) }} as int64) in ({{ brasil.valid_ddds | join(', ') }})
    )
    -- Adicionar validações para outros países aqui
  )
{% endmacro %}