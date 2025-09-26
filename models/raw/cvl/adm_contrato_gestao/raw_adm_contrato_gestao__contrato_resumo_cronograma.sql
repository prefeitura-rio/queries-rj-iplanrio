{{
    config(
        alias='contrato_resumo_cronograma',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_contrato_mes), r'\.0$', '') AS INT64) AS id_contrato_mes,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_contrato), r'\.0$', '') AS INT64) AS id_contrato,
  SAFE_CAST(TRIM(mes) AS INT64) AS mes,
  SAFE_CAST(TRIM(valor) AS NUMERIC) AS valor
FROM {{ source('brutos_osinfo_staging', 'contrato_resumo_cronograma') }} AS t
