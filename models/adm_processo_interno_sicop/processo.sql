SELECT
  SAFE_CAST(
    REGEXP_REPLACE(p.org_transc, r'\.0$', '') AS STRING
  ) org_transc,
  SAFE_CAST(
    REGEXP_REPLACE(p.num_processo, r'\.0$', '') AS STRING
  ) num_processo,
  SAFE_CAST(
    REGEXP_REPLACE(p.cpf_cgc, r'\.0$', '') AS STRING
  ) cpf_cgc,
  SAFE_CAST(
    REGEXP_REPLACE(p.ident, r'\.0$', '') AS STRING
  ) ident,
  SAFE_CAST(
    REGEXP_REPLACE(p.requerente, r'\.0$', '') AS STRING
  ) requerente,
  SAFE_CAST(
    REGEXP_REPLACE(p.dia_proc2, r'\.0$', '') AS INT64
  ) dia_proc2,
  SAFE_CAST(
    REGEXP_REPLACE(p.mes_proc2, r'\.0$', '') AS INT64
  ) mes_proc2,
  SAFE_CAST(
    REGEXP_REPLACE(p.sec_proc2, r'\.0$', '') AS INT64
  ) sec_proc2,
  SAFE_CAST(
    REGEXP_REPLACE(p.ano_proc2, r'\.0$', '') AS INT64
  ) ano_proc2,
    SAFE_CAST(
    REGEXP_REPLACE(p.ano_sist2, r'\.0$', '') AS INT64
  ) ano_sist2,
  SAFE_CAST(
    REGEXP_REPLACE(p.sec_sist2, r'\.0$', '') AS INT64
  ) sec_sist2,
  SAFE_CAST(
    REGEXP_REPLACE(p.mes_sist2, r'\.0$', '') AS INT64
  ) mes_sist2,
  SAFE_CAST(
    REGEXP_REPLACE(p.dia_sist2, r'\.0$', '') AS INT64
  ) dia_sist2,
  SAFE_CAST(
    REGEXP_REPLACE(p.desc_assun, r'\.0$', '') AS STRING
  ) desc_assun,
  SAFE_CAST(
    REGEXP_REPLACE(p.mat_transc, r'\.0$', '') AS STRING
  ) mat_transc,
  SAFE_CAST(
    REGEXP_REPLACE(p.status, r'\.0$', '') AS STRING
  ) status
 FROM {{ source('adm_processo_interno_sicop_staging', 'processo') }}
