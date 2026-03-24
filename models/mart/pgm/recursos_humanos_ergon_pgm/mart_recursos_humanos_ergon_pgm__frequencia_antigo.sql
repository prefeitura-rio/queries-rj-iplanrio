{{
    config(
        alias='frequencia_antigo',
        materialized="table",
        tags=["mart", "ergon", "frequencia"],
        description="requência de servidores da Prefeitura da Cidade do Rio de Janeiro cadastrada em sistemas de recursos humanos antigos."
    )
}}

SELECT
    t.id_matricula_vinculo,
    t.id_frequencia,
    t.data_frequencia
FROM {{ ref('raw_recursos_humanos_ergon__frequencia_antigo') }} AS t
inner join {{ ref('mart_recursos_humanos_ergon_pgm__vinculo') }} AS v on v.id_matricula_vinculo = t.id_matricula_vinculo


