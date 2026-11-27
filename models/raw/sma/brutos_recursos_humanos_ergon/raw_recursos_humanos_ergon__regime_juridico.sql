{{
    config(
        alias='regime_juridico',
        materialized="table",
        tags=["raw", "ergon", "regime_juridico"],
        description="Tipos de regimes jurídicos existentes entre os vínculos de funcionários e a administração direta ou indireta da prefeitura do Rio de Janeiro."
    )
}}

SELECT
    SAFE_CAST(sigla AS STRING) AS sigla,
    SAFE_CAST(nome AS STRING) AS nome
FROM {{ source('brutos_recursos_humanos_ergon', 'vw_dlk_erg_regimes_jur_') }} AS t