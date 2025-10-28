{{
    config(
        alias='fechamento_estoque',
        description="Fechamento mensal do estoque"
    )
}}

SELECT
  SAFE_CAST(cd_unidade_armazenadora AS DECIMAL) AS id_unidade_armazenadora,
  SAFE_CAST(ano_mes_referencia AS STRING) AS ano_mes_referencia,
  SAFE_CAST(saldo_anterior AS DECIMAL) AS saldo_anterior,
  SAFE_CAST(entrada_por_alienacao AS DECIMAL) AS total_entrada_por_alienacao,
  SAFE_CAST(entrada_por_compra AS DECIMAL) AS total_entrada_por_compra,
  SAFE_CAST(entrada_por_devolucao AS DECIMAL) AS total_entrada_por_devolucao,
  SAFE_CAST(entrada_por_ajuste_contabil AS DECIMAL) AS total_entrada_por_ajuste_contabil,
  SAFE_CAST(entrada_por_incorporacao AS DECIMAL) AS total_entrada_por_incorporacao,
  SAFE_CAST(entrada_por_transferencia AS DECIMAL) AS total_entrada_por_transferencia,
  SAFE_CAST(saida_por_consumo AS DECIMAL) AS total_saida_por_consumo,
  SAFE_CAST(saida_por_transferencia AS DECIMAL) AS total_saida_por_transferencia,
  SAFE_CAST(saida_por_ajuste_contabil AS DECIMAL) AS total_saida_por_ajuste_contabil,
  SAFE_CAST(saida_por_degaste_natural AS DECIMAL) AS total_saida_por_degaste_natural,
  SAFE_CAST(saida_por_alienacao AS DECIMAL) AS total_saida_por_alienacao,
  SAFE_CAST(saida_por_baixa AS DECIMAL) AS total_saida_por_baixa,
  SAFE_CAST(total_estorno AS DECIMAL) AS total_estorno,
  SAFE_CAST(acerto_por_pmu AS DECIMAL) AS valor_acerto_preco_medio_unitario,
  SAFE_CAST(residuo_contabil AS DECIMAL) AS residuo_contabil,
  SAFE_CAST(salto_atual AS DECIMAL) AS saldo_atual
FROM {{ source('brutos_compras_materiais_servicos_sigma_staging', 'fechamento_estoque') }}