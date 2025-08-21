{{
    config(
        alias='sancao_fornecedor_compras',
        schema='compras_materiais_servicos_sigma'
    )
}}

SELECT
  SAFE_CAST(CPF_CNPJ AS INT64) AS cpf_cnpj,
  SAFE_CAST(RAZAO_SOCIAL AS STRING) AS razao_social,
  SAFE_CAST(NR_ORDEM AS INT64) AS numero_ordem,
  SAFE_CAST(PROCESSO_ORIGEM AS INT64) AS numero_processo_origem,
  SAFE_CAST(PROCESSO_INSTRUTIVO AS INT64) AS id_processo_instrutivo,
  SAFE_CAST(PROCESSO_FATURA AS INT64) AS numero_processo_fatura,
  SAFE_CAST(CD_SANCAO AS INT64) AS codigo_sancao,
  SAFE_CAST(DS_SANCAO AS STRING) AS descricao_sancao,
  SAFE_CAST(DT_SANCAO AS STRING) AS data_sancao,
  SAFE_CAST(DT_EXTINCAO_SANCAO AS STRING) AS data_extincao_sancao

from {{ source('compras_materiais_servicos_sigma_staging', 'VW_SANCAO_ADMINISTRATIVA')}}