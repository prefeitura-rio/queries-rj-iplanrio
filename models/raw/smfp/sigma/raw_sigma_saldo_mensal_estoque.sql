{{
    config(
        alias='saldo_mensal_estoque',
        schema='compras_materiais_servicos_sigma'
    )
}}

SELECT
  SAFE_CAST(cd_unidade_armazenadora AS INT64) AS id_unidade_armazenadora,
  SAFE_CAST(ano_mes_referencia AS STRING) AS ano_mes_referencia,
  SAFE_CAST(cd_material AS STRING) AS id_material,
  SAFE_CAST(valor AS FLOAT64) AS saldo_valor_material,
  SAFE_CAST(qtd_baixas AS FLOAT64) AS quantidade_baixas_material,
  SAFE_CAST(quantidade AS FLOAT64) AS saldo_quantidade_material,
  SAFE_CAST(qtd_entrada AS FLOAT64) AS quantidade_entrada_material,
  SAFE_CAST(qtd_saida AS FLOAT64) AS quantidade_saida_material
  from {{ source('compras_materiais_servicos_sigma_staging', 'VW_SALDO_MENSAL_ESTOQUE')}}