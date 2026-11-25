{#
    Lê uma tabela de raw_nfse_nacional (nfse, dps, pessoas, eventos) sempre com filtro na coluna de partição
    (data_hora_geracao, partição diária do parser), para o BigQuery nunca varrer a tabela inteira por engano.
    A data inicial vem de var('nc2_raw_desde') (padrão 2026-01-01, o IniExtDocsFiscais de produção): uma nota só
    entra se a competência do serviço é >= IniExtDocsFiscais, e o documento nunca é gerado antes da competência.
    Linhas sem data_hora_geracao (partição __NULL__) continuam entrando.
#}
{% macro nc2_raw(tabela) -%}
    (
        select *
        from {{ nc2_fonte("raw_nfse_nacional", tabela) }}
        where
            data_hora_geracao >= timestamp("{{ var('nc2_raw_desde', '2026-01-01') }}")
            or data_hora_geracao is null
    )
{%- endmacro %}
