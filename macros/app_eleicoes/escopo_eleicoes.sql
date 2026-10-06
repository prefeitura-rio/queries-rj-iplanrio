{# Recorte de anos e cargos do app de eleições. As colunas `ano` e `cargo` devem existir na query. #}
{% macro filtro_escopo_eleicoes() -%}
    (
        (ano in (2022, 2026) and cargo in ('presidente', 'governador'))
        or (ano = 2024 and cargo = 'prefeito')
    )
{%- endmacro %}
