SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_usuario_sistema), r'\.0$', '') AS STRING) AS id_usuario_sistema,
  SAFE_CAST(TRIM(cod_usuario) AS STRING) AS cod_usuario,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_sistema), r'\.0$', '') AS STRING) AS id_sistema,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_perfil), r'\.0$', '') AS STRING) AS id_perfil,
  SAFE_CAST(DATE(data_inicial) AS DATE) AS data_inicial,
  SAFE_CAST(DATE(data_final) AS DATE) AS data_final
FROM {{ source('brutos_osinfo_staging', 'usuario_sistema') }}