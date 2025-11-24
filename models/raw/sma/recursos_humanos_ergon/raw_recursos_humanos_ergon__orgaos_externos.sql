{{
    config(
        alias='orgaos_externos',
    )
}}

SELECT
    SAFE_CAST(orgao AS STRING) AS nome,
    SAFE_CAST(descr AS STRING) AS nome_completo,
    SAFE_CAST(tipo_orgao AS STRING) AS tipo,
FROM {{ source('brutos_ergon_staging', 'VW_DLK_ERG_ORGAOS_EXTERNOS') }} AS t