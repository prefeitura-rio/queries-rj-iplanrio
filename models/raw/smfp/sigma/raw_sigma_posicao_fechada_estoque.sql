{{
    config(
        alias='posicao_fechada_estoque',
        schema='compras_materiais_servicos_sigma'
    )
}}

SELECT
  SAFE_CAST(cd_unidade_armazenadora AS INT64) AS id_unidade_armazenadora,
  SAFE_CAST(ano_mes_referencia AS STRING) AS ano_mes_referencia,
  SAFE_CAST(cd_material AS STRING) AS id_material,
  SAFE_CAST(estoque_mes AS FLOAT64) AS quantidade_material_estoque_mes,
  SAFE_CAST(preco_medio AS FLOAT64) AS preco_medio_material,
  SAFE_CAST(valor_estoque AS FLOAT64) AS valor_total_material_estoque

from {{ source('compras_materiais_servicos_sigma_staging', 'VW_POSICAO_FECHADA_ESTOQUE')}}