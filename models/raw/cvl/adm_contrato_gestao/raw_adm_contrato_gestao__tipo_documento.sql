SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_tipo_documento), r'\.0$', '') AS STRING) AS id_tipo_documento,
  SAFE_CAST(TRIM(tipo_documento) AS STRING) AS tipo_documento,
  SAFE_CAST(TRIM(documento) AS STRING) AS documento
FROM {{ source('brutos_osinfo_staging', 'tipo_documento') }}