SELECT
  SAFE_CAST(
    REGEXP_REPLACE(t.org_resp, r'\.0$', '') AS STRING
  ) org_resp,
  SAFE_CAST(
    REGEXP_REPLACE(t.tipo_doc, r'\.0$', '') AS STRING
  ) tipo_doc,
  SAFE_CAST(
    REGEXP_REPLACE(t.num_documento, r'\.0$', '') AS STRING
  ) num_documento,
  SAFE_CAST(
    REGEXP_REPLACE(t.seq, r'\.0$', '') AS INT64
  ) seq,
  SAFE_CAST(
    REGEXP_REPLACE(t.sec_remes2, r'\.0$', '') AS INT64
  ) sec_remes2,
  SAFE_CAST(
    REGEXP_REPLACE(t.ano_remes2, r'\.0$', '') AS INT64
  ) ano_remes2,
  SAFE_CAST(
    REGEXP_REPLACE(t.mes_remes2, r'\.0$', '') AS INT64
  ) mmes_remes2,
  SAFE_CAST(
    REGEXP_REPLACE(t.dia_remes2, r'\.0$', '') AS INT64
  ) dia_remes2,
  SAFE_CAST(
    REGEXP_REPLACE(t.destino, r'\.0$', '') AS STRING
  ) destino,
  SAFE_CAST(
    REGEXP_REPLACE(t.cod_desp, r'\.0$', '') AS STRING
  ) cod_desp,
 FROM {{ source('adm_processo_interno_sicop_staging', 'tramitacao_documento') }}
