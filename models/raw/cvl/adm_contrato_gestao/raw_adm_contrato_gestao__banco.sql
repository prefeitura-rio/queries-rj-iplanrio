{{
    config(
        alias='banco',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_banco AS STRING)), r'\.0$', '') AS STRING) AS id_banco,
  SAFE_CAST(TRIM(SAFE_CAST(cod_banco AS STRING)) AS STRING) AS cod_banco,
  SAFE_CAST(TRIM(SAFE_CAST(banco AS STRING)) AS STRING) AS banco,
  SAFE_CAST(TRIM(SAFE_CAST(nome_fantasia AS STRING)) AS STRING) AS nome_fantasia,
  SAFE_CAST(TRIM(SAFE_CAST(flg_ativo AS STRING)) AS STRING) AS flg_ativo
FROM {{ source('brutos_osinfo_staging', 'banco') }} AS t
