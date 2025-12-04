{{
    config(
        alias='tipo_orgao',
    )
}}

SELECT
    SAFE_CAST(tipo AS STRING) AS tipo,
    SAFE_CAST(descr AS STRING) AS descricao,
FROM {{ source('brutos_ergon_staging', 'VW_DLK_ERG_TIPO_ORGAO') }} AS t