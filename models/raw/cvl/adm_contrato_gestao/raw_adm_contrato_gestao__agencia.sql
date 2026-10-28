{{
    config(
        alias='agencia',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_agencia AS STRING)), r'\.0$', '') AS STRING) AS id_agencia,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_banco AS STRING)), r'\.0$', '') AS STRING) AS id_banco,
  SAFE_CAST(TRIM(SAFE_CAST(numero_agencia AS STRING)) AS STRING) AS numero_agencia,
  SAFE_CAST(TRIM(SAFE_CAST(digito AS STRING)) AS STRING) AS digito,
  SAFE_CAST(TRIM(SAFE_CAST(agencia AS STRING)) AS STRING) AS agencia,
  SAFE_CAST(TRIM(SAFE_CAST(flg_ativo AS STRING)) AS STRING) AS flg_ativo
FROM {{ source('brutos_osinfo_staging', 'agencia') }} AS t
