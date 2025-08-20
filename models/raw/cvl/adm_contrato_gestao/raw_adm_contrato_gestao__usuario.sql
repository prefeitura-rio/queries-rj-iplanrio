SELECT
  SAFE_CAST(TRIM(cod_usuario) AS STRING) AS cod_usuario,
  SAFE_CAST(TRIM(cod_unidade) AS STRING) AS cod_unidade,
  SAFE_CAST(TRIM(login) AS STRING) AS login,
  SAFE_CAST(TRIM(nome) AS STRING) AS nome,
  SAFE_CAST(DATE(data_cadastro) AS DATE) AS data_cadastro,
  SAFE_CAST(DATE(data_atualizacao) AS DATE) AS data_atualizacao,
  SAFE_CAST(DATE(data_exclusao) AS DATE) AS data_exclusao,
  SAFE_CAST(TRIM(flg_excluido) AS STRING) AS flg_excluido,
  SAFE_CAST(TRIM(cargo) AS STRING) AS cargo
FROM {{ source('brutos_osinfo_staging', 'usuario') }}