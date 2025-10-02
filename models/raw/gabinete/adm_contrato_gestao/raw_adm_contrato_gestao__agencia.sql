{{
    config(
        alias='agencia',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_agencia), r'\.0$', '') AS STRING) AS id_agencia,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_banco), r'\.0$', '') AS STRING) AS id_banco,
  SAFE_CAST(TRIM(numero_agencia) AS STRING) AS numero_agencia,
  SAFE_CAST(TRIM(digito) AS STRING) AS digito,
  SAFE_CAST(TRIM(agencia) AS STRING) AS agencia,
  SAFE_CAST(TRIM(flg_ativo) AS STRING) AS flg_ativo
FROM {{ source('brutos_osinfo_staging', 'agencia') }} AS t