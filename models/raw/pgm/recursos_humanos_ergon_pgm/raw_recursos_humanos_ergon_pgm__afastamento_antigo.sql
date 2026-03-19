{{
    config(
        alias='afastamento_antigo',
        materialized="table",
        tags=["raw", "ergon", "afastamento"],
        description="Tabela que armazena as ocorrências de frequência do servidor municipal."
    )
}}

SELECT
    id_matricula_vinculo,
    data_inicio,
    data_previsao_retorno,
    data_fim
FROM {{ ref('raw_recursos_humanos_ergon__afastamento_antigo') }} AS t
inner join {{ ref('raw_recursos_humanos_ergon_pgm__vinculo') }} AS v on v.id_matricula_vinculo = t.id_matricula_vinculo