{{
    config(
        alias='pre_contagem',
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


