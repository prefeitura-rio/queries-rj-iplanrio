{{
    config(
        alias='tipo_vinculo',
    )
}}

SELECT
    SAFE_CAST(sigla AS STRING) AS sigla,
    SAFE_CAST(nome AS STRING) AS nome,
FROM {{ source('brutos_ergon_staging', 'VW_DLK_ERG_TIPO_VINC_') }} AS t