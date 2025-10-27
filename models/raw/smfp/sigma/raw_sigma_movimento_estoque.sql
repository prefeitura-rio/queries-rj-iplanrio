{{
    config(
        alias='movimento_estoque',
        description="Movimento do estoque"
    )
}}

SELECT
  SAFE_CAST(cd_unidade_origem AS NUMERIC) AS id_unidade_origem,
  SAFE_CAST(ano_mes_referencia AS STRING) AS ano_mes_referencia,
  SAFE_CAST(cd_material AS STRING) AS id_material,
  SAFE_CAST(tipo_movimentacao AS STRING) AS tipo_movimentacao,
  SAFE_CAST(dt_movimento AS STRING) AS data_movimento,
  SAFE_CAST(cd_unidade_destino AS NUMERIC) AS id_unidade_destino,
  SAFE_CAST(tp_documento AS STRING) AS tipo_documento,
  SAFE_CAST(nota_fiscal AS NUMERIC) AS nota_fiscal_fornecedor,
  SAFE_CAST(cnpj_fornecedor AS STRING) AS cnpj_fornecedor,
  SAFE_CAST(num_doc_unidade_destino AS NUMERIC) AS numero_documento_unidade_destino,
  SAFE_CAST(num_doc_unidade_origem AS NUMERIC) AS numero_documento_unidade_origem,
  SAFE_CAST(quant AS NUMERIC) AS quantidade_material_movimentada,
  SAFE_CAST(valor_unitario AS NUMERIC) AS valor_unitario_material_movimentado,
  SAFE_CAST(vl_total AS NUMERIC) AS valor_total_material_movimentado,
  SAFE_CAST(acerto_pmu AS NUMERIC) AS valor_acerto_preco_medio_unitario
FROM {{ source('brutos_compras_materiais_servicos_sigma_staging', 'movimento_estoque') }}