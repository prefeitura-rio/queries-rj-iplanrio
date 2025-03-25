SELECT
  SAFE_CAST(
    REGEXP_REPLACE(o.org_sicop, r'\.0$', '') AS INT64
  ) org_sicop,
  SAFE_CAST(
    REGEXP_REPLACE(o.cod_orcto, r'\.0$', '') AS INT64
  ) cod_orcto
 FROM {{ source('adm_processo_interno_sicop_staging', 'orgao') }}
