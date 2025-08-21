{{
    config(
        alias='material_em_transito',
        schema='compras_materiais_servicos_sigma'
    )
}}

SELECT
  SAFE_CAST(cd_unidade_origem AS INT64) AS id_unidade_origem,
  SAFE_CAST(ano_mes_referencia AS STRING) AS ano_mes_referencia,
  SAFE_CAST(cd_unidade_destino AS INT64) AS id_unidade_destino,
  SAFE_CAST(tp_documento AS STRING) AS tipo_documento,
  SAFE_CAST(num_doc_unidade_origem AS INT64) AS numero_documento_unidade_origem,
  SAFE_CAST(cd_material AS STRING) AS id_material,
  SAFE_CAST(st_movimentacao AS STRING) AS status_movimentacao,
  SAFE_CAST(quantidade AS FLOAT64) AS quantidade_material_transito,
  SAFE_CAST(valor_unitario AS FLOAT64) AS valor_unitario_material_transito
from {{ source('compras_materiais_servicos_sigma_staging', 'VW_MATERIAL_EM_TRANSITO')}}