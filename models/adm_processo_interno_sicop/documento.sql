SELECT
  SAFE_CAST(
    REGEXP_REPLACE(d.org_resp, r'\.0$', '') AS STRING
  ) org_resp,
  SAFE_CAST(
    REGEXP_REPLACE(d.org_transc, r'\.0$', '') AS STRING
  ) org_transc,
  SAFE_CAST(
    REGEXP_REPLACE(d.tipo_doc, r'\.0$', '') AS STRING
  ) tipo_doc,
  SAFE_CAST(
    REGEXP_REPLACE(d.sec_sist2, r'\.0$', '') AS INT64
  ) sec_sist2,
  SAFE_CAST(
    REGEXP_REPLACE(d.ano_sist2, r'\.0$', '') AS INT64
  ) ano_sist2,
  SAFE_CAST(
    REGEXP_REPLACE(d.mes_sist2, r'\.0$', '') AS INT64
  ) mes_sist2,
  SAFE_CAST(
    REGEXP_REPLACE(d.dia_sist2, r'\.0$', '') AS INT64
  ) dia_sist2,
  SAFE_CAST(
    REGEXP_REPLACE(d.num_documento, r'\.0$', '') AS STRING
  ) num_documento,
  SAFE_CAST(
    REGEXP_REPLACE(d.requerente, r'\.0$', '') AS STRING
  ) requerente,
  SAFE_CAST(
    REGEXP_REPLACE(d.cod_assun, r'\.0$', '') AS STRING
  ) cod_assun,
  SAFE_CAST(
    REGEXP_REPLACE(d.mat_transc, r'\.0$', '') AS STRING
  ) mat_transc,
  SAFE_CAST(
    REGEXP_REPLACE(d.assun_comp, r'\.0$', '') AS STRING
  ) assun_comp
 FROM {{ source('adm_processo_interno_sicop_staging', 'documento') }}
