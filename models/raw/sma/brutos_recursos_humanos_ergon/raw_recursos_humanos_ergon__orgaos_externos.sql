{{
    config(
        alias='orgaos_externos',
        materialized="table",
        tags=["raw", "ergon", "orgaos_externos", "orgaos"],
        description="Órgãos de outros entes federativos que não pertencem à prefeitura do Rio de Janeiro, mas que são de interesse da prefeitura do Rio de Janeiro."
    )
}}

SELECT
    SAFE_CAST(orgao AS STRING) AS nome,
    SAFE_CAST(descr AS STRING) AS nome_completo,
    SAFE_CAST(tipo_orgao AS STRING) AS tipo
FROM {{ source('brutos_recursos_humanos_ergon', 'vw_dlk_erg_orgaos_externos') }} AS t