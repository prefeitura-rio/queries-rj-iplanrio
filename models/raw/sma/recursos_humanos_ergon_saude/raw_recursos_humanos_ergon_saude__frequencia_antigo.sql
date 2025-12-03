{{
    config(
        alias='frequencia_antigo',
    )
}}

SELECT
    SAFE_CAST(M9 AS STRING) AS id_matricula_vinculo,
    SAFE_CAST(SF_OCORRENCIA AS STRING) AS id_frequencia,
    SAFE_CAST(SF_DT_OC_Y2 AS STRING) AS data_frequencia,
FROM {{ source('brutos_ergon_saude_staging', 'VW_SIMPA_SIRHU_FREQUENCIA_GBP') }} AS t