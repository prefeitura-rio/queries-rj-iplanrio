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
FROM {{ source('brutos_recursos_humanos_ergon', 'vw_dlk_erg_orgaos_regimes_jur_') }} AS t