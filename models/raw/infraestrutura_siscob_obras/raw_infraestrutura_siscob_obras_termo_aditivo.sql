{{
    config(
        alias="termo_aditivo",
        schema='infraestrutura_siscob_obras',
        materialized="table",
    )
}}

SELECT 
    DISTINCT
        SAFE_CAST(REGEXP_REPLACE(cd_obra, r'\.0$', '') AS STRING) id_obra,
        SAFE_CAST(tp_acerto AS STRING) tipo_acerto,
        SAFE_CAST(
            SAFE.PARSE_TIMESTAMP ('%Y-%m-%d %H:%M:%S', dt_do) AS DATETIME
        ) AS data_publicacao,
        SAFE_CAST(
            SAFE.PARSE_TIMESTAMP ('%Y-%m-%d %H:%M:%S', dt_autorizacao) AS DATETIME
        ) AS data_autorizacao,
        SAFE_CAST(vl_acerto AS FLOAT64) valor_acerto,
FROM {{ source('brutos_siscob_staging', 'termo_aditivo') }}