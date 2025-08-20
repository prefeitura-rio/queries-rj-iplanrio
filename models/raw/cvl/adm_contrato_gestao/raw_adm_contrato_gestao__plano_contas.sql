SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_despesa), r'\.0$', '') AS STRING) AS id_item_plano_de_contas,
  SAFE_CAST(TRIM(cod_despesa) AS STRING) AS cod_item_plano_de_contas,
  SAFE_CAST(TRIM(despesa) AS STRING) AS descricao_item_plano_de_contas,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_despesa_n1), r'\.0$', '') AS STRING) AS id_item_plano_de_contas_n1,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_despesa_n2), r'\.0$', '') AS STRING) AS id_item_plano_de_contas_n2,
  SAFE_CAST(TRIM(flg_ativo) AS STRING) AS flg_ativo
FROM {{ source('brutos_osinfo_staging', 'plano_contas') }}