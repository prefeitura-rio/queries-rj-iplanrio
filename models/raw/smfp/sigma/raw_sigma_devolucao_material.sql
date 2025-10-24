{{
    config(
        alias='devolucao_material',
        description="Devolução de material"
    )
}}

SELECT
  SAFE_CAST(cd_unidade_origem AS DECIMAL) AS id_unidade_origem,
  SAFE_CAST(ano_mes_referencia AS STRING) AS ano_mes_referencia,
  SAFE_CAST(cd_material AS STRING) AS id_material,
  SAFE_CAST(tipo_movimentacao AS STRING) AS tipo_movimentacao,
  SAFE_CAST(dt_movimento AS STRING) AS data_movimento,
  SAFE_CAST(cd_unidade_destino AS DECIMAL) AS id_unidade_destino,
  SAFE_CAST(tp_documento AS STRING) AS tipo_documento,
  SAFE_CAST(num_doc_unidade_origem AS DECIMAL) AS numero_documento_unidade_origem,
  SAFE_CAST(quant AS DECIMAL) AS quantidade_material_devolvida,
  SAFE_CAST(valor_unitario AS DECIMAL) AS valor_unitario_material_devolvido,
  SAFE_CAST(vl_total AS DECIMAL) AS valor_total_material_devolvido

from {{ source('brutos_compras_materiais_servicos_sigma_staging', 'devolucao_material')}}