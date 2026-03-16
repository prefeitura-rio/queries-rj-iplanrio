{{
    config(
        alias='frequencia_antigo',
    )
}}

SELECT
    SAFE_CAST(M9 AS STRING) AS id_matricula_vinculo,
    SAFE_CAST(SF_OCORRENCIA AS int64) AS id_frequencia,
    safe_cast(SAFE_CAST(SF_DT_OC_Y2 AS timestamp) as date) AS data_frequencia
FROM {{ source('brutos_ergon_staging', 'VW_SIMPA_SIRHU_FREQUENCIA_GBP') }} AS t