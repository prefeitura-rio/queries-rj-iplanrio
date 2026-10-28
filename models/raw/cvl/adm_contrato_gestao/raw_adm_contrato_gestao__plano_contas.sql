{{
    config(
        alias='plano_contas',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_despesa AS STRING)), r'\.0$', '') AS STRING) AS id_item_plano_de_contas,
  SAFE_CAST(TRIM(SAFE_CAST(cod_despesa AS STRING)) AS STRING) AS cod_item_plano_de_contas,
  SAFE_CAST(TRIM(SAFE_CAST(despesa AS STRING)) AS STRING) AS descricao_item_plano_de_contas,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_despesa_n1 AS STRING)), r'\.0$', '') AS STRING) AS id_item_plano_de_contas_n1,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_despesa_n2 AS STRING)), r'\.0$', '') AS STRING) AS id_item_plano_de_contas_n2,
  SAFE_CAST(TRIM(SAFE_CAST(flg_ativo AS STRING)) AS STRING) AS flg_ativo
FROM {{ source('brutos_osinfo_staging', 'plano_contas') }} AS t
