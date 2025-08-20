SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_rubrica), r'\.0$', '') AS STRING) AS id_rubrica,
  SAFE_CAST(TRIM(rubrica) AS STRING) AS rubrica,
  SAFE_CAST(TRIM(flg_ativo) AS STRING) AS flg_ativo
FROM {{ source('brutos_osinfo_staging', 'rubrica') }}