{{
    config(
        alias='indicador'
    )
}}

SELECT
    SAFE_CAST(metaID AS STRING) as id_meta,
    SAFE_CAST(ano AS INT64) as ano,
    SAFE_CAST(mes AS INT64) as mes,
    SAFE_CAST(valorRealizado AS FLOAT64) as valor
FROM {{ source('planejamento_gestao_dashboard_metas_staging', 'indicador') }}
