{{
    config(
        alias='averbacoes',
        materialized="table",
        tags=["raw", "ergon", "averbacoes"],
        description="Tabela que armazena o cadastro das averbações."
    )
}}

SELECT
    id_funcionario,
    id_vinculo,
    id_averbacao,
    data_inicio,
    data_final,
    instituicao,
    id_tipo_tempo,
    data_validade,
    total_dias_averbados,
    motivo,
    sobrepoe,
    id_empresa,
    obs,
    regime_previdenciario
FROM {{ ref('raw_recursos_humanos_ergon__averbacoes') }} AS t
inner join {{ ref('raw_recursos_humanos_ergon_pgm__vinculo') }} AS v on v.id_funcionario = t.NUMFUNC and t.NUMVINC = v.id_vinculo