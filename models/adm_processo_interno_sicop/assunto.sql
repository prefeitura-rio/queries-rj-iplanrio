SELECT
  SAFE_CAST(
    REGEXP_REPLACE(a.ident, r'\.0$', '') AS INT64
  ) ident,
  SAFE_CAST(
    REGEXP_REPLACE(a.cod, r'\.0$', '') AS STRING
  ) cod,
  SAFE_CAST(
    REGEXP_REPLACE(a.seq, r'\.0$', '') AS INT64
  ) seq,
  SAFE_CAST(
    REGEXP_REPLACE(a.desc_assunto, r'\.0$', '') AS STRING
  ) desc_assunto
 FROM {{ source('adm_processo_interno_sicop_staging', 'assunto') }}
