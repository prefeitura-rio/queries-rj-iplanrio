{{
    config(
        alias='contrato_resumo_cronograma',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  {{ clean_and_cast('id_contrato_mes', 'int64', trim=true) }} AS id_contrato_mes,
  {{ clean_and_cast('id_contrato', 'int64', trim=true) }} AS id_contrato,
  SAFE_CAST(TRIM(mes) AS INT64) AS mes,
  SAFE_CAST(TRIM(valor) AS NUMERIC) AS valor
FROM {{ source('brutos_osinfo_staging', 'contrato_resumo_cronograma') }} AS t
