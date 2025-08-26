{% macro classify_phone_type(ddi, ddd, numero) %}
  {% set config = var('phone_validation') %}
  {% set brasil = config.countries.brasil %}
  case
    when {{ ddi }} is null then null
    when {{ ddi }} = '{{ brasil.ddi }}' 
         and length({{ numero }}) = {{ brasil.celular_length }}
         and starts_with({{ numero }}, '{{ brasil.celular_prefix }}')
    then 'CELULAR'
    
    when {{ ddi }} = '{{ brasil.ddi }}' 
         and length({{ numero }}) = {{ brasil.fixo_length }}
    then 'FIXO'
    
    when {{ ddi }} != '{{ brasil.ddi }}'
    then 'OUTROS'  -- Internacional
    
    else 'OUTROS'
  end
{% endmacro %}