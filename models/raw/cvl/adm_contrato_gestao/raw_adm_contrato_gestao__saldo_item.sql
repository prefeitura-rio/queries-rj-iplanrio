SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_saldo_item), r'\.0$', '') AS STRING) AS id_saldo_item,
  SAFE_CAST(TRIM(saldo_item) AS STRING) AS saldo_item,
  SAFE_CAST(TRIM(flg_ativo) AS STRING) AS flg_ativo,
  SAFE_CAST(REGEXP_REPLACE(TRIM(ordem), r'\.0$', '') AS INT64) AS ordem,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_saldo_tipo), r'\.0$', '') AS STRING) AS id_saldo_tipo
FROM {{ source('brutos_osinfo_staging', 'saldo_item') }}