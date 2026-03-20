{{
    config(
        alias='plano_contas',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  {{ clean_and_cast('id_despesa', 'string', trim=true) }} AS id_item_plano_de_contas,
  SAFE_CAST(TRIM(cod_despesa) AS STRING) AS cod_item_plano_de_contas,
  SAFE_CAST(TRIM(despesa) AS STRING) AS descricao_item_plano_de_contas,
  {{ clean_and_cast('id_despesa_n1', 'string', trim=true) }} AS id_item_plano_de_contas_n1,
  {{ clean_and_cast('id_despesa_n2', 'string', trim=true) }} AS id_item_plano_de_contas_n2,
  SAFE_CAST(TRIM(flg_ativo) AS STRING) AS flg_ativo
FROM {{ source('brutos_osinfo_staging', 'plano_contas') }} AS t