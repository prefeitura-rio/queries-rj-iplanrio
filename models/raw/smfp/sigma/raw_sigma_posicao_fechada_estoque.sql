{{
    config(
        alias='posicao_fechada_estoque',
        description="Posição fechada do estoque"
    )
}}

SELECT
  SAFE_CAST(cd_unidade_armazenadora AS NUMERIC) AS id_unidade_armazenadora,
  SAFE_CAST(ano_mes_referencia AS STRING) AS ano_mes_referencia,
  SAFE_CAST(cd_material AS STRING) AS id_material,
  SAFE_CAST(estoque_mes AS NUMERIC) AS quantidade_material_estoque_mes,
  SAFE_CAST(preco_medio AS NUMERIC) AS preco_medio_material,
  SAFE_CAST(valor_estoque AS NUMERIC) AS valor_total_material_estoque
FROM {{ source('brutos_compras_materiais_servicos_sigma_staging', 'posicao_fechada_estoque') }}