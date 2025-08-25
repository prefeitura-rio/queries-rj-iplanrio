{{
    config(
        alias='servico',
        schema='compras_materiais_servicos_sigma'
    )
}}

SELECT
  SAFE_CAST(CD_SERV AS STRING) AS codigo_servico,
  SAFE_CAST(CD_SEQ AS STRING) AS codigo_sequencia,
  SAFE_CAST(DV AS STRING) AS digito_verificador,
  SAFE_CAST(CD_SERVICO AS STRING) AS id_servico,
  SAFE_CAST(DS_SERVICO AS STRING) AS descricao_servico,
  SAFE_CAST(ST_STATUS AS STRING) AS status_servico,
  SAFE_CAST(CD_COMPRASNET AS STRING) AS codigo_compras_net,
  SAFE_CAST(NM_PADRONIZADO AS STRING) AS nome_padronizado,
  SAFE_CAST(UNIDADE_SERVICO AS STRING) AS unidade_servico,
  SAFE_CAST(ST_RESPONSAVEL_TECNICO AS STRING) AS responsavel_tecnico,
  SAFE_CAST(ST_SISTEMA_REGISTRO_PRECO AS STRING) AS registro_preco,
  SAFE_CAST(CD_ATIVIDADE_ECONOMICA AS STRING) AS id_atividade_economica,
  SAFE_CAST(CD_GRUPO_CAE AS STRING) AS codigo_grupo_cae,
  SAFE_CAST(CD_SUBGRUPO_CAE AS STRING) AS codigo_subgrupo_cae,
  SAFE_CAST(CD_ATIVIDADE_CAE AS STRING) AS codigo_atividade_economica,
  SAFE_CAST(DS_ATIVIDADE_ECONOMICA AS STRING) AS descricao_atividade_economica,
  SAFE_CAST(DS_SUBGRUPO_CAE AS STRING) AS descricao_subgrupo_cae,
  SAFE_CAST(ST_TABELADO AS STRING) AS situacao_tabelado,
  SAFE_CAST(ST_CADASTRO_FORNECEDOR AS STRING) AS situacao_cadastro_fornecedor
from {{ source('compras_materiais_servicos_sigma_staging', 'VW_SERVICO')}}