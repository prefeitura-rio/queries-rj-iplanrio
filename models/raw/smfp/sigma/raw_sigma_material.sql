{{
    config(
        alias='material',
        schema='compras_materiais_servicos_sigma'
    )
}}

SELECT
  SAFE_CAST(CD_MATERIAL AS STRING) AS codigo_material,
  SAFE_CAST(CD_GRUPO AS STRING) AS codigo_grupo,
  SAFE_CAST(CD_CLASSE AS STRING) AS codigo_classe,
  SAFE_CAST(CD_SUBCLASSE AS STRING) AS codigo_subclasse,
  SAFE_CAST(SEQUENCIAL AS STRING) AS codigo_sequencial,
  SAFE_CAST(DV1 AS STRING) AS digito_verificador_1,
  SAFE_CAST(DV2 AS STRING) AS digito_verificador_2,
  SAFE_CAST(NM_PADRONIZADO AS STRING) AS nome_padronizado_material,
  SAFE_CAST(NM_COMPLEMENTAR_MATERIAL AS STRING) AS nome_complementar_material,
  SAFE_CAST(UNIDADE AS STRING) AS unidade_consumo,
  SAFE_CAST(DS_DETALHE_MATERIAL AS STRING) AS descricao_detalhada,
  SAFE_CAST(DT_DESATIVACAO AS STRING) AS data_desativacao,
  SAFE_CAST(ST_STATUS AS STRING) AS status_material,
  SAFE_CAST(REMUME AS STRING) AS tabela_remume,
  SAFE_CAST(ACONDICIONAMENTO AS STRING) AS acondicionamento,
  SAFE_CAST(TP_GENERO AS STRING) AS genero_alimenticio,
  SAFE_CAST(CD_MATERIAL_SUBSTITUTO AS INT64) AS codigo_material_substituto,
  SAFE_CAST(NM_FANTASIA AS STRING) AS nome_fantasia,
  SAFE_CAST(CD_COMPRASNET AS INT64) AS codigo_compras_net,
  SAFE_CAST(TP_MATERIAL AS STRING) AS tipo_material,
  SAFE_CAST(ST_REFERENCIA AS STRING) AS referencia,
  SAFE_CAST(ST_SISTEMA_REGISTRO_PRECO AS STRING) AS registro_preco,
  SAFE_CAST(TERMOLABEL AS STRING) AS medicamento_refrigerado,
  SAFE_CAST(ST_CONTROLADO AS STRING) AS medicamento_controlado,
  SAFE_CAST(ST_PADRONIZADO AS STRING) AS medicamento_padronizado,
  SAFE_CAST(ST_USO_GERAL AS STRING) AS medicamento_uso_geral,
  SAFE_CAST(ST_CONTINUADO AS STRING) AS medicamento_continuado,
  SAFE_CAST(ST_TABELADO AS STRING) AS medicamento_tabelado,
  SAFE_CAST(ST_ITEM_SUSTENTAVEL AS STRING) AS item_sustentavel,
  SAFE_CAST(OBSERVACAO AS STRING) AS observacao
from {{ source('compras_materiais_servicos_sigma_staging', 'VW_MATERIAL')}}