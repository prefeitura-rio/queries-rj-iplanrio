{{
    config(
        alias='rubrica',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_rubrica AS STRING)), r'\.0$', '') AS STRING) AS id_rubrica,
  SAFE_CAST(TRIM(SAFE_CAST(rubrica AS STRING)) AS STRING) AS rubrica,
  SAFE_CAST(TRIM(SAFE_CAST(flg_ativo AS STRING)) AS STRING) AS flg_ativo
FROM `rj-iplanrio.brutos_osinfo_staging.rubrica` AS t
