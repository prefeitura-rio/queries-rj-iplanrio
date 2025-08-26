{% macro get_nationality(ddi) %}
  case
    when {{ ddi }} is null then null
    when {{ ddi }} = '55' then 'Brasil'
    when {{ ddi }} = '1' then 'Estados Unidos'
    when {{ ddi }} = '54' then 'Argentina'
    -- Expandir conforme necessário
    else 'Internacional'
  end
{% endmacro %}