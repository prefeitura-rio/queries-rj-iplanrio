{{
    config(
        alias='contrato_resumo_cronograma',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_contrato_mes AS STRING)), r'\.0$', '') AS INT64) AS id_contrato_mes,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_contrato AS STRING)), r'\.0$', '') AS INT64) AS id_contrato,
  SAFE_CAST(TRIM(SAFE_CAST(mes AS STRING)) AS INT64) AS mes,
  SAFE_CAST(TRIM(SAFE_CAST(valor AS STRING)) AS NUMERIC) AS valor
FROM {{ source('brutos_osinfo_staging', 'contrato_resumo_cronograma') }} AS t
