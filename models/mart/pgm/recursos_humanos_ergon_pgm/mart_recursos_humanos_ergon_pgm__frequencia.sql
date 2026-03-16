
{{
    config(
        alias='frequencia',
        materialized="table",
        tags=["raw", "ergon", "frequencia"],
        description="Tabela que armazena as ocorrências de frequência do servidor municipal."
    )
}}

SELECT
    t.id_funcionario,
    t.id_vinculo,
    t.data_inicio,
    t.data_final,
    t.tipo_frequencia,
    t.id_frequencia,
    t.observacoes,
    t.id_empresa
FROM {{ ref('raw_recursos_humanos_ergon__frequencia') }} AS t
inner join {{ ref('mart_recursos_humanos_ergon_pgm__vinculo') }} AS v on v.id_funcionario = t.id_funcionario and t.id_vinculo = v.id_vinculo
