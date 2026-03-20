{{
    config(
        alias='rubrica',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  {{ clean_and_cast('id_rubrica', 'string', trim=true) }} AS id_rubrica,
  SAFE_CAST(TRIM(rubrica) AS STRING) AS rubrica,
  SAFE_CAST(TRIM(flg_ativo) AS STRING) AS flg_ativo
FROM `rj-iplanrio.brutos_osinfo_staging.rubrica` AS t