{{
    config(
        alias='secretaria',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  {{ clean_and_cast('id_secretaria', 'string', trim=true) }} AS id_secretaria,
  SAFE_CAST(TRIM(secretaria) AS STRING) AS secretaria,
  SAFE_CAST(TRIM(sigla) AS STRING) AS sigla,
  SAFE_CAST(TRIM(regional) AS STRING) AS regional,
  SAFE_CAST(TRIM(sigla_regional) AS STRING) AS sigla_regional,
  SAFE_CAST(TRIM(cod_secretaria) AS STRING) AS cod_secretaria,
  SAFE_CAST(TRIM(flg_regional) AS STRING) AS flg_regional
FROM {{ source('brutos_osinfo_staging', 'secretaria') }} AS t