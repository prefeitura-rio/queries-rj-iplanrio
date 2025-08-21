{{
    config(
        alias='fornecedor',
        schema='compras_materiais_servicos_sigma'
    )
}}

SELECT
  SAFE_CAST(CPF_CNPJ AS STRING) AS cpf_cnpj,
  SAFE_CAST(TIPO_CPF_CNPJ AS STRING) AS tipo_fornecedor,
  SAFE_CAST(INSCRICAO_MUNICIPAL AS STRING) AS inscricao_municipal,
  SAFE_CAST(INSCRICAO_ESTADUAL AS FLOAT64) AS inscricao_estadual,
  SAFE_CAST(RAZAO_SOCIAL AS STRING) AS razao_social,
  SAFE_CAST(NOME_FANTASIA AS STRING) AS nome_fantasia,
  SAFE_CAST(NOME_CONTATO AS STRING) AS nome_contato,
  SAFE_CAST(EMAIL AS STRING) AS email,
  SAFE_CAST(EMAIL_CONTATO AS STRING) AS email_contato,
  SAFE_CAST(FAX AS FLOAT64) AS fax_fornecedor,
  SAFE_CAST(DDD AS INT64) AS ddd,
  SAFE_CAST(DDI AS INT64) AS ddi,
  SAFE_CAST(RAMAL AS INT64) AS ramal,
  SAFE_CAST(TELEFONE AS INT64) AS telefone,
  SAFE_CAST(LOGRADOURO AS STRING) AS logradouro,
  SAFE_CAST(NUMERO_PORTA AS INT64) AS numero_porta,
  SAFE_CAST(COMPLEMENTO AS STRING) AS complemento_endereco,
  SAFE_CAST(BAIRRO AS STRING) AS bairro,
  SAFE_CAST(MUNICIPIO AS STRING) AS municipio,
  SAFE_CAST(UF AS STRING) AS uf,
  SAFE_CAST(CEP AS INT64) AS cep,
  SAFE_CAST(ATIVO_INATIVO_BLOQUEADO AS STRING) AS status_fornecedor,
  SAFE_CAST(CD_NATUREZA_JURIDICA AS INT64) AS codigo_natureza_juridica
from {{ source('compras_materiais_servicos_sigma_staging', 'VW_FORNECEDOR')}}