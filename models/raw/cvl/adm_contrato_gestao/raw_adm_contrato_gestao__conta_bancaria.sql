{{
    config(
        alias='conta_bancaria',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}


SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_conta_bancaria AS STRING)), r'\.0$', '') AS STRING) AS id_conta_bancaria,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_agencia AS STRING)), r'\.0$', '') AS STRING) AS id_agencia,
  SAFE_CAST(TRIM(SAFE_CAST(codigo_cc AS STRING)) AS STRING) AS codigo_cc,
  SAFE_CAST(TRIM(SAFE_CAST(digito_cc AS STRING)) AS STRING) AS digito_cc,
  SAFE_CAST(TRIM(SAFE_CAST(flg_ativo AS STRING)) AS STRING) AS flg_ativo,
  SAFE_CAST(TRIM(SAFE_CAST(cod_organizacao AS STRING)) AS STRING) AS cod_instituicao,
  SAFE_CAST(TRIM(SAFE_CAST(cod_tipo AS STRING)) AS STRING) AS cod_tipo
FROM {{ source('brutos_osinfo_staging', 'conta_bancaria') }} AS t
