{{
    config(
        alias='termo_aditivo_resumo_cronograma',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  {{ clean_and_cast('id_termo_aditivo_mes', 'int64', trim=true) }} AS id_termo_aditivo_mes,
  {{ clean_and_cast('id_termo_aditivo', 'int64', trim=true) }} AS id_termo_aditivo,
  SAFE_CAST(TRIM(mes) AS INT64) AS mes,
  SAFE_CAST(TRIM(valor) AS NUMERIC) AS valor
FROM {{ source('brutos_osinfo_staging', 'termo_aditivo_resumo_cronograma') }} AS t
