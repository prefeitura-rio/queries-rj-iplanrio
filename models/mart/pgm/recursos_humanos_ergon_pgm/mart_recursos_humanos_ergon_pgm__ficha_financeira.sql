{{
    config(
        materialized='table',
        alias='ficha_financeira',
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
        tags=["raw", "ergon", "ficha_financeira", "salarios"],
        description="Tabela que contém os registros de valores de rubricas de fichas financeiras dos funcionários da Prefeitura da Cidade do Rio de Janeiro."
    )
}}

SELECT
    t.mes_ano_folha,
    t.numero_folha,
    t.id_funcionario,
    t.id_vinculo,
    t.id_pensionista,
    t.mes_ano_direito,
    t.id_rubrica,
    t.tipo_rubrica,
    t.desconto_vantagem,
    t.observacao,
    t.valor,
    t.correcao,
    t.id_empresa,
    t.data_particao
FROM {{ ref('raw_recursos_humanos_ergon__ficha_financeira') }} AS t
inner join {{ ref('mart_recursos_humanos_ergon_pgm__vinculo') }} AS v on v.id_funcionario = t.id_funcionario and t.id_vinculo = v.id_vinculo



