{{
    config(
        alias='tipo_tempo',
        materialized="table",
        tags=["raw", "ergon", "tipo", "tipo_tempo"],
        description="Tabela armazena os tipos de tempo utilizados para contagem de benefícios categorizados como finalidades."
    )
}}

SELECT
    safe_cast(sigla as string) as id_tipo_tempo,
    SAFE_CAST(nome AS STRING) AS nome_tipo_tempo,
    safe_cast(aposentadoria as string) as aposentadoria,
    SAFE_CAST(ferias AS string) AS ferias,
    SAFE_CAST(dias_fer AS string) AS dias_de_ferias,
    SAFE_CAST(adictserv AS string) AS trienio,
    SAFE_CAST(licesp AS string) AS licenca_especial,
    SAFE_CAST(dias_licesp AS string) AS dias_de_licenca_especial,
    SAFE_CAST(adictchefia AS string) AS tempo_de_chefia,
    SAFE_CAST(progressao AS string) AS progressao
FROM {{ source('brutos_ergon_staging', 'TIPO_TEMPO') }} AS t