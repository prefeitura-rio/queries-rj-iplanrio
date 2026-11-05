{{
    config(
        alias="termo_aditivo",
        schema='infraestrutura_siscob_obras',
        materialized="table",
    )
}}

SELECT 
    DISTINCT
        SAFE_CAST(cd_obra AS STRING) id_obra,
        SAFE_CAST(tp_acerto AS STRING) tipo_acerto,
        SAFE_CAST(
            dt_do AS DATETIME
        ) AS data_publicacao,
        SAFE_CAST(
            dt_autorizacao AS DATETIME
        ) AS data_autorizacao,
        SAFE_CAST(vl_acerto AS FLOAT64) valor_acerto,
FROM {{ source('brutos_siscob_staging', 'termo_aditivo') }} AS t