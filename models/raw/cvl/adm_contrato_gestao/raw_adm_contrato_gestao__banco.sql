{{
    config(
        alias='banco',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  {{ clean_and_cast('id_banco', 'string', trim=true) }} AS id_banco,
  SAFE_CAST(TRIM(cod_banco) AS STRING) AS cod_banco,
  SAFE_CAST(TRIM(banco) AS STRING) AS banco,
  SAFE_CAST(TRIM(nome_fantasia) AS STRING) AS nome_fantasia,
  SAFE_CAST(TRIM(flg_ativo) AS STRING) AS flg_ativo
FROM {{ source('brutos_osinfo_staging', 'banco') }} AS t