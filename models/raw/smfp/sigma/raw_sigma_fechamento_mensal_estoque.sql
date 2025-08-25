{{
    config(
        alias='fechamento_mensal_estoque',
        schema='compras_materiais_servicos_sigma'
    )
}}

SELECT
  SAFE_CAST(cd_unidade_armazenadora AS INT64) AS id_unidade_armazenadora,
  SAFE_CAST(ano_mes_referencia AS STRING) AS ano_mes_referencia,
  SAFE_CAST(saldo_anterior AS FLOAT64) AS saldo_anterior,
  SAFE_CAST(entrada_por_alienacao AS FLOAT64) AS total_entrada_por_alienacao,
  SAFE_CAST(entrada_por_compra AS FLOAT64) AS total_entrada_por_compra,
  SAFE_CAST(entrada_por_devolucao AS FLOAT64) AS total_entrada_por_devolucao,
  SAFE_CAST(entrada_por_ajuste_contabil AS FLOAT64) AS total_entrada_por_ajuste_contabil,
  SAFE_CAST(entrada_por_incorporacao AS FLOAT64) AS total_entrada_por_incorporacao,
  SAFE_CAST(entrada_por_transferencia AS FLOAT64) AS total_entrada_por_transferencia,
  SAFE_CAST(saida_por_consumo AS FLOAT64) AS total_saida_por_consumo,
  SAFE_CAST(saida_por_transferencia AS FLOAT64) AS total_saida_por_transferencia,
  SAFE_CAST(saida_por_ajuste_contabil AS FLOAT64) AS total_saida_por_ajuste_contabil,
  SAFE_CAST(saida_por_degaste_natural AS FLOAT64) AS total_saida_por_degaste_natural,
  SAFE_CAST(saida_por_alienacao AS FLOAT64) AS total_saida_por_alienacao,
  SAFE_CAST(saida_por_baixa AS FLOAT64) AS total_saida_por_baixa,
  SAFE_CAST(total_estorno AS FLOAT64) AS total_estorno,
  SAFE_CAST(acerto_por_pmu AS FLOAT64) AS valor_acerto_preco_medio_unitario,
  SAFE_CAST(residuo_contabil AS FLOAT64) AS residuo_contabil,
  SAFE_CAST(salto_atual AS FLOAT64) AS saldo_atual

from {{ source('compras_materiais_servicos_sigma_staging', 'VW_FECHAMENTO_ESTOQUE')}}