{{
    config(
        alias='fornecedor',
        description="Cadastro de Fornecedores de bens e serviços"
    )
}}

SELECT
  SAFE_CAST(CPF_CNPJ AS STRING) AS cpf_cnpj,
  SAFE_CAST(TIPO_CPF_CNPJ AS STRING) AS tipo_fornecedor,
  SAFE_CAST(INSCRICAO_MUNICIPAL AS STRING) AS inscricao_municipal,
  SAFE_CAST(INSCRICAO_ESTADUAL AS BIGDECIMAL) AS inscricao_estadual,
  SAFE_CAST(RAZAO_SOCIAL AS STRING) AS razao_social,
  SAFE_CAST(NOME_FANTASIA AS STRING) AS nome_fantasia,
  SAFE_CAST(NOME_CONTATO AS STRING) AS nome_contato,
  SAFE_CAST(EMAIL AS STRING) AS email,
  SAFE_CAST(EMAIL_CONTATO AS STRING) AS email_contato,
  SAFE_CAST(FAX AS BIGDECIMAL) AS fax_fornecedor,
  SAFE_CAST(DDD AS DECIMAL) AS ddd,
  SAFE_CAST(DDI AS DECIMAL) AS ddi,
  SAFE_CAST(RAMAL AS DECIMAL) AS ramal,
  SAFE_CAST(TELEFONE AS BIGDECIMAL) AS telefone,
  SAFE_CAST(LOGRADOURO AS STRING) AS logradouro,
  SAFE_CAST(NUMERO_PORTA AS DECIMAL) AS numero_porta,
  SAFE_CAST(COMPLEMENTO AS STRING) AS complemento_endereco,
  SAFE_CAST(BAIRRO AS STRING) AS bairro,
  SAFE_CAST(MUNICIPIO AS STRING) AS municipio,
  SAFE_CAST(UF AS STRING) AS uf,
  SAFE_CAST(CEP AS DECIMAL) AS cep,
  SAFE_CAST(ATIVO_INATIVO_BLOQUEADO AS STRING) AS status_fornecedor,
  SAFE_CAST(CD_NATUREZA_JURIDICA AS DECIMAL) AS codigo_natureza_juridica,
  SAFE_CAST(DS_NATUREZA_JURIDICA AS STRING) AS descricao_natureza_juridica,
  SAFE_CAST(CD_RAMO_ATIVIDADE AS DECIMAL) AS codigo_ramo_atividade,
  SAFE_CAST(DS_RAMO_ATIVIDADE AS STRING) AS descricao_ramo_atividade,
  SAFE_CAST(CD_PORTE_EMPRESA AS DECIMAL) AS codigo_porte_fornecedor,
  SAFE_CAST(DS_PORTE_EMPRESA AS STRING) AS descricao_porte_fornecedor,
  SAFE_CAST(DATA_ULTIMA_ATUALIZACAO AS DECIMAL) AS data_ultima_atualizacao,
  SAFE_CAST(TIPO_FORNECEDOR AS STRING) AS tipo_cadastro_fornecedor,

from {{ source('brutos_compras_materiais_servicos_sigma_staging', 'fornecedor')}}