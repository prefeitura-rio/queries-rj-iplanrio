{{
    config(
        alias='averbacoes_contagem',
        materialized="table",
        tags=["raw", "ergon", "averbacoes", "contagem"],
        description="Tabela que armazena informações referentes sobre finalidade de contagem e total do tempo de contagem referentes a períodos averbados pelos servidores."
    )
}}

SELECT
    id_funcionario,
    id_vinculo,
    id_averbacao,
    finalidade,
    dias
FROM {{ ref('raw_recursos_humanos_ergon__averbacoes_contagem') }} AS t
inner join {{ ref('raw_recursos_humanos_ergon_pgm__vinculo') }} AS v on v.id_funcionario = t.id_funcionario and t.id_vinculo = v.id_vinculo