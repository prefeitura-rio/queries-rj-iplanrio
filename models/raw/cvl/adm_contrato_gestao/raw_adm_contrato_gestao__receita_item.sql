{{
    config(
        alias='receita_item',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  {{ clean_and_cast('id_receita_item', 'string', trim=true) }} AS id_receita_item,
  SAFE_CAST(TRIM(receita_item) AS STRING) AS receita_item,
  SAFE_CAST(TRIM(flg_ativo) AS STRING) AS flg_ativo,
  {{ clean_and_cast('ordem', 'int64', trim=true) }} AS ordem,
  {{ clean_and_cast('id_receita_tipo', 'string', trim=true) }} AS id_receita_tipo
FROM {{ source('brutos_osinfo_staging', 'receita_item') }} AS t