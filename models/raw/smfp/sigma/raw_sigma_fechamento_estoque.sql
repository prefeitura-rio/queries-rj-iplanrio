{{
    config(
        alias='fechamento_mensal_estoque',
        description="Fechamento mensal do estoque"
    )
}}

SELECT
  SAFE_CAST(cd_unidade_armazenadora AS NUMERIC) AS id_unidade_armazenadora,
  SAFE_CAST(ano_mes_referencia AS STRING) AS ano_mes_referencia,
  SAFE_CAST(saldo_anterior AS NUMERIC) AS saldo_anterior,
  SAFE_CAST(entrada_por_alienacao AS NUMERIC) AS total_entrada_por_alienacao,
  SAFE_CAST(entrada_por_compra AS NUMERIC) AS total_entrada_por_compra,
  SAFE_CAST(entrada_por_devolucao AS NUMERIC) AS total_entrada_por_devolucao,
  SAFE_CAST(entrada_por_ajuste_contabil AS NUMERIC) AS total_entrada_por_ajuste_contabil,
  SAFE_CAST(entrada_por_incorporacao AS NUMERIC) AS total_entrada_por_incorporacao,
  SAFE_CAST(entrada_por_transferencia AS NUMERIC) AS total_entrada_por_transferencia,
  SAFE_CAST(saida_por_consumo AS NUMERIC) AS total_saida_por_consumo,
  SAFE_CAST(saida_por_transferencia AS NUMERIC) AS total_saida_por_transferencia,
  SAFE_CAST(saida_por_ajuste_contabil AS NUMERIC) AS total_saida_por_ajuste_contabil,
  SAFE_CAST(saida_por_degaste_natural AS NUMERIC) AS total_saida_por_degaste_natural,
  SAFE_CAST(saida_por_alienacao AS NUMERIC) AS total_saida_por_alienacao,
  SAFE_CAST(saida_por_baixa AS NUMERIC) AS total_saida_por_baixa,
  SAFE_CAST(total_estorno AS NUMERIC) AS total_estorno,
  SAFE_CAST(acerto_por_pmu AS NUMERIC) AS valor_acerto_preco_medio_unitario,
  SAFE_CAST(residuo_contabil AS NUMERIC) AS residuo_contabil,
  SAFE_CAST(salto_atual AS NUMERIC) AS saldo_atual
FROM {{ source('brutos_compras_materiais_servicos_sigma_staging', 'fechamento_estoque') }}