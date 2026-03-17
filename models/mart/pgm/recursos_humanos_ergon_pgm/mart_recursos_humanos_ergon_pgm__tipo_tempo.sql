{{
    config(
        alias='tipo_tempo',
        materialized="table",
        tags=["raw", "ergon", "tipo", "tipo_tempo"],
        description="Tabela armazena os tipos de tempo utilizados para contagem de benefícios categorizados como finalidades."
    )
}}

SELECT
    id_tipo_tempo,
    nome_tipo_tempo,
    aposentadoria,
    ferias,
    dias_de_ferias,
    trienio,
    licenca_especial,
    dias_de_licenca_especial,
    tempo_de_chefia,
    progressao
FROM {{ ref('raw_recursos_humanos_ergon__tipo_tempo') }} AS t