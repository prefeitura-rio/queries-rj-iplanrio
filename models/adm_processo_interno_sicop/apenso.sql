SELECT
  SAFE_CAST(
    REGEXP_REPLACE(a.chave, r'\.0$', '') AS STRING
  ) chave,
  SAFE_CAST(
    REGEXP_REPLACE(a.num_processo_principal, r'\.0$', '') AS STRING
  ) num_processo_principal,
  SAFE_CAST(
    REGEXP_REPLACE(a.num_processo_apensado, r'\.0$', '') AS STRING
  ) num_processo_apensado,
  SAFE_CAST(
    REGEXP_REPLACE(a.i22005_cod_oper, r'\.0$', '') AS INT64
  ) i22005_cod_oper
 FROM {{ source('adm_processo_interno_sicop_staging', 'apenso') }}
