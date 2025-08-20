{% macro validate_phone_quality(numero_completo, freq_usage) %}
  {% set config = var('phone_validation') %}
  case
    when {{ is_format_valid(numero_completo) }} 
         and {{ freq_usage }} <= cast({{ config.freq_valid_max }} as int64)
    then 'VALIDO'
    
    when {{ is_format_valid(numero_completo) }} 
         and (
           {{ freq_usage }} between cast({{ config.freq_suspicious_min }} as int64) and cast({{ config.freq_suspicious_max }} as int64)
           or {{ has_suspicious_patterns(numero_completo) }}
         )
    then 'SUSPEITO'
    
    when not {{ is_format_valid(numero_completo) }}
         or {{ freq_usage }} >= cast({{ config.freq_invalid_min }} as int64)
         or {{ has_dummy_patterns(numero_completo) }}
    then 'INVALIDO'
    
    else 'SUSPEITO'
  end
{% endmacro %}