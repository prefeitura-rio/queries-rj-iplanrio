{{
    config(
        alias='saldo_item',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  {{ clean_and_cast('id_saldo_item', 'string', trim=true) }} AS id_saldo_item,
  SAFE_CAST(TRIM(saldo_item) AS STRING) AS saldo_item,
  SAFE_CAST(TRIM(flg_ativo) AS STRING) AS flg_ativo,
  {{ clean_and_cast('ordem', 'int64', trim=true) }} AS ordem,
  {{ clean_and_cast('id_saldo_tipo', 'string', trim=true) }} AS id_saldo_tipo
FROM {{ source('brutos_osinfo_staging', 'saldo_item') }} AS t