{{
    config(
        alias='frequencia_antigo',
    )
}}

SELECT
    SAFE_CAST(REGEXP_REPLACE(TRIM(m9), r'\.0$', '') AS STRING) AS id_matricula_vinculo,
    SAFE_CAST(REGEXP_REPLACE(TRIM(sf_ocorrencia), r'\.0$', '') AS STRING) AS id_frequencia,
    SAFE_CAST(TRIM(sf_dt_oc_y2) AS STRING) AS data_frequencia,
FROM {{ source('recursos_humanos_ergon', 'frequencia_antigo') }} AS t