{{
    config(
        alias='material_em_transito',
        description="Material em trânsito"
    )
}}

SELECT
  SAFE_CAST(cd_unidade_origem AS NUMERIC) AS id_unidade_origem,
  SAFE_CAST(ano_mes_referencia AS STRING) AS ano_mes_referencia,
  SAFE_CAST(cd_unidade_destino AS NUMERIC) AS id_unidade_destino,
  SAFE_CAST(tp_documento AS STRING) AS tipo_documento,
  SAFE_CAST(num_doc_unidade_origem AS NUMERIC) AS numero_documento_unidade_origem,
  SAFE_CAST(cd_material AS STRING) AS id_material,
  SAFE_CAST(st_movimentacao AS STRING) AS status_movimentacao,
  SAFE_CAST(quantidade AS NUMERIC) AS quantidade_material_transito,
  SAFE_CAST(valor_unitario AS NUMERIC) AS valor_unitario_material_transito
FROM {{ source('brutos_compras_materiais_servicos_sigma_staging', 'material_em_transito') }}