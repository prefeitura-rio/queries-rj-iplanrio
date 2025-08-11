{{
    config(
        alias='afastamento_antigo',
    )
}}

SELECT
    SAFE_CAST(REGEXP_REPLACE(TRIM(m10), r'\.0$', '') AS STRING) AS id_matricula_vinculo,
    SAFE_CAST(TRIM(sa_dt_afas_y2) AS STRING) AS data_inicio,
    SAFE_CAST(TRIM(sa_dt_prer_y2) AS STRING) AS data_previsao_retorno,
    SAFE_CAST(TRIM(sa_dt_retr_y2) AS STRING) AS data_fim,
FROM {{ source('recursos_humanos_ergon', 'afastamento_antigo') }} AS t