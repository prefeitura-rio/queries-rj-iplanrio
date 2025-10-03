{{
    config(
        alias='termo_aditivo_resumo_cronograma',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_termo_aditivo_mes), r'\.0$', '') AS INT64) AS id_termo_aditivo_mes,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_termo_aditivo), r'\.0$', '') AS INT64) AS id_termo_aditivo,
  SAFE_CAST(TRIM(mes) AS INT64) AS mes,
  SAFE_CAST(TRIM(valor) AS NUMERIC) AS valor
FROM {{ source('brutos_osinfo_staging', 'termo_aditivo_resumo_cronograma') }} AS t
