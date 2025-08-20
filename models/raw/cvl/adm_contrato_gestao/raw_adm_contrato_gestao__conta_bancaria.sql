SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_conta_bancaria), r'\.0$', '') AS STRING) AS id_conta_bancaria,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_agencia), r'\.0$', '') AS STRING) AS id_agencia,
  SAFE_CAST(TRIM(codigo_cc) AS STRING) AS codigo_cc,
  SAFE_CAST(TRIM(digito_cc) AS STRING) AS digito_cc,
  SAFE_CAST(TRIM(flg_ativo) AS STRING) AS flg_ativo,
  SAFE_CAST(TRIM(cod_organizacao) AS STRING) AS cod_instituicao,
  SAFE_CAST(TRIM(cod_tipo) AS STRING) AS cod_tipo
FROM {{ source('brutos_osinfo_staging', 'conta_bancaria') }}