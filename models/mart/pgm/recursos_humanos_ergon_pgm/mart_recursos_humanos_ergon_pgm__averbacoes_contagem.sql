{{
    config(
        alias='averbacoes_contagem',
        materialized="table",
        tags=["mart", "ergon", "averbacoes", "contagem"],
        description="Tabela que armazena informações referentes sobre finalidade de contagem e total do tempo de contagem referentes a períodos averbados pelos servidores."
    )
}}

SELECT
    t.id_funcionario,
    t.id_vinculo,
    t.id_averbacao,
    t.finalidade,
    t.dias
FROM {{ ref('raw_recursos_humanos_ergon__averbacoes_contagem') }} AS t
inner join {{ ref('mart_recursos_humanos_ergon_pgm__vinculo') }} AS v on v.id_funcionario = t.id_funcionario and t.id_vinculo = v.id_vinculo


