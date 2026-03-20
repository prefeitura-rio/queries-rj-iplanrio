{{
    config(
        alias='averbacoes',
        materialized="table",
        tags=["raw", "ergon", "averbacoes"],
        description="Tabela que armazena o cadastro das averbações."
    )
}}

SELECT
    t.id_funcionario,
    t.id_vinculo,
    t.id_averbacao,
    t.data_inicio,
    t.data_final,
    t.instituicao,
    t.id_tipo_tempo,
    t.data_validade,
    t.total_dias_averbados,
    t.motivo,
    t.sobrepoe,
    t.id_empresa,
    t.obs,
    t.regime_previdenciario
FROM {{ ref('raw_recursos_humanos_ergon__averbacoes') }} AS t
inner join {{ ref('raw_recursos_humanos_ergon_pgm__vinculo') }} AS v on v.id_funcionario = t.id_funcionario and t.id_vinculo = v.id_vinculo