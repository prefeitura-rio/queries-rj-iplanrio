{{
    config(
        alias='pre_contagem',
        materialized="table",
        tags=["mart", "ergon", "pre_contagem", "contagem"],
        description="A tabela armazena informações de contagens anteriores ao início do sistema recursos humanos ou da entrada do servidor no sistema, se por algum motivo não possuía informações anteriormente."
    )
}}

SELECT
    t.finalidade,
    t.id_funcionario,
    t.id_vinculo,
    t.periodos,
    t.dias,
    t.data_validade,
    t.id_empresa,
    t.observacoes
FROM {{ ref('raw_recursos_humanos_ergon__pre_contagem') }} AS t
inner join {{ ref('mart_recursos_humanos_ergon_pgm__vinculo') }} AS v on v.id_funcionario = t.id_funcionario and v.id_vinculo = t.id_vinculo


