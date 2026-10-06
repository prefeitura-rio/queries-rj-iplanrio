{# Chave de zona: IBGE do município + zona com 3 dígitos. Estável entre 2022, 2024 e 2026. #}
{% macro id_zona(id_municipio, zona) -%}
    concat({{ id_municipio }}, '-', lpad(cast(safe_cast({{ zona }} as int64) as string), 3, '0'))
{%- endmacro %}

{# Chave de seção: id_zona + seção com 4 dígitos. #}
{% macro id_secao(id_municipio, zona, secao) -%}
    concat(
        {{ id_zona(id_municipio, zona) }},
        '-',
        lpad(cast(safe_cast({{ secao }} as int64) as string), 4, '0')
    )
{%- endmacro %}

{# Chave de local de votação: id_zona + número do local com 4 dígitos. Única por ano. #}
{% macro id_local_votacao(id_municipio, zona, numero_local) -%}
    concat(
        {{ id_zona(id_municipio, zona) }},
        '-',
        lpad(cast(safe_cast({{ numero_local }} as int64) as string), 4, '0')
    )
{%- endmacro %}
