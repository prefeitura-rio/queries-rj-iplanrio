{{
    config(
        alias='agencia',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  {{ clean_and_cast('id_agencia', 'string', trim=true) }} AS id_agencia,
  {{ clean_and_cast('id_banco', 'string', trim=true) }} AS id_banco,
  SAFE_CAST(TRIM(numero_agencia) AS STRING) AS numero_agencia,
  SAFE_CAST(TRIM(digito) AS STRING) AS digito,
  SAFE_CAST(TRIM(agencia) AS STRING) AS agencia,
  SAFE_CAST(TRIM(flg_ativo) AS STRING) AS flg_ativo
FROM {{ source('brutos_osinfo_staging', 'agencia') }} AS t