{{
    config(
        alias='secretaria',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_secretaria AS STRING)), r'\.0$', '') AS STRING) AS id_secretaria,
  SAFE_CAST(TRIM(SAFE_CAST(secretaria AS STRING)) AS STRING) AS secretaria,
  SAFE_CAST(TRIM(SAFE_CAST(sigla AS STRING)) AS STRING) AS sigla,
  SAFE_CAST(TRIM(SAFE_CAST(regional AS STRING)) AS STRING) AS regional,
  SAFE_CAST(TRIM(SAFE_CAST(sigla_regional AS STRING)) AS STRING) AS sigla_regional,
  SAFE_CAST(TRIM(SAFE_CAST(cod_secretaria AS STRING)) AS STRING) AS cod_secretaria,
  SAFE_CAST(TRIM(SAFE_CAST(flg_regional AS STRING)) AS STRING) AS flg_regional
FROM {{ source('brutos_osinfo_staging', 'secretaria') }} AS t
