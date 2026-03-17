{{
    config(
        alias='orgaos_regime_juridico',
        materialized="table",
        tags=["raw", "ergon", "regime_juridico", "orgaos"],
        description="Regimes jurídicos que podem ser aceitos para contratação de funcionários nos órgãos da administração direta ou indireta da prefeitura do Rio de Janeiro."
    )
}}

SELECT
    SAFE_CAST(sigla AS STRING) AS sigla,
    SAFE_CAST(nome AS STRING) AS nome
FROM {{ source('brutos_ergon_staging', 'VW_DLK_ERG_ORGAOS_REGIMES_JUR_') }} AS t