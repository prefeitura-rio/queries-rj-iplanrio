{{
    config(
        alias='saldo_item',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_saldo_item AS STRING)), r'\.0$', '') AS STRING) AS id_saldo_item,
  SAFE_CAST(TRIM(SAFE_CAST(saldo_item AS STRING)) AS STRING) AS saldo_item,
  SAFE_CAST(TRIM(SAFE_CAST(flg_ativo AS STRING)) AS STRING) AS flg_ativo,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(ordem AS STRING)), r'\.0$', '') AS INT64) AS ordem,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_saldo_tipo AS STRING)), r'\.0$', '') AS STRING) AS id_saldo_tipo
FROM {{ source('brutos_osinfo_staging', 'saldo_item') }} AS t
