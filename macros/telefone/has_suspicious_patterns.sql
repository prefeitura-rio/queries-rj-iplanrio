{% macro has_suspicious_patterns(numero_completo) %}
  {% set config = var('phone_validation') %}
  (
    -- Repetição excessiva de dígitos (BigQuery regex syntax)
    regexp_contains({{ numero_completo }}, r'(\d)\\1{' || {{ config.max_repeated_digits }} || ',}')
    
    -- Padrões suspeitos específicos
    {% for pattern in config.suspicious_patterns %}
    or strpos({{ numero_completo }}, '{{ pattern }}') > 0
    {% endfor %}
  )
{% endmacro %}