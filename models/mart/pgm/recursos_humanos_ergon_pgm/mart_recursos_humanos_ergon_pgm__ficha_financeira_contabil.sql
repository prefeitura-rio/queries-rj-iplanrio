{{
    config(
        materialized='table',
        alias='ficha_financeira_contabil',
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
        tags=["mart", "ergon", "ficha_financeira_contabil", "salarios"],
        description="Tabela que contém os registros de valores de rubricas de fichas financeiras dos funcionários da Prefeitura da Cidade do Rio de Janeiro."
    )
}}

SELECT
    t.mes_ano_folha,
    t.id_funcionario,
    t.id_vinculo,
    t.id_pensionista,
    t.numero_folha,
    t.id_setor,
    t.id_secretaria,
    t.tipo_funcionario,
    t.detalhamento,
    t.id_rubrica,
    t.tipo_rubrica,
    t.mes_ano_direito,
    t.desconto_vantagem,
    t.valor,
    t.observacao,
    t.tipo_classificacao,
    t.classificacao,
    t.id_empresa,
    t.data_particao
FROM {{ ref('raw_recursos_humanos_ergon__ficha_financeira_contabil') }} AS t
inner join {{ ref('mart_recursos_humanos_ergon_pgm__vinculo') }} AS v on v.id_funcionario = t.id_funcionario and t.id_vinculo = v.id_vinculo





