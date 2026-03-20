{{
    config(
        alias='conta_bancaria',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}


SELECT
  {{ clean_and_cast('id_conta_bancaria', 'string', trim=true) }} AS id_conta_bancaria,
  {{ clean_and_cast('id_agencia', 'string', trim=true) }} AS id_agencia,
  SAFE_CAST(TRIM(codigo_cc) AS STRING) AS codigo_cc,
  SAFE_CAST(TRIM(digito_cc) AS STRING) AS digito_cc,
  SAFE_CAST(TRIM(flg_ativo) AS STRING) AS flg_ativo,
  SAFE_CAST(TRIM(cod_organizacao) AS STRING) AS cod_instituicao,
  SAFE_CAST(TRIM(cod_tipo) AS STRING) AS cod_tipo
FROM {{ source('brutos_osinfo_staging', 'conta_bancaria') }} AS t