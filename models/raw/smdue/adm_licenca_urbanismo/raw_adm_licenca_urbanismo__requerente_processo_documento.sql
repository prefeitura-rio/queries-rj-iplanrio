{{
  config(
    materialized='table',
    alias='requerente_processo_documento'
  )
}}

SELECT
codPessoa AS id_requerente,
nomPessoa AS nome_requerente,
telPessoa AS telefone_requerente,
EmailPessoa AS email_requerente,
CPF_CNPJ AS CPF_CNPJ,
CAST(vfFisica AS BOOL) AS pessoa_fisica,
identidade AS identidade_requerente,
emissor AS emissor_identidade,
cep AS cep,
CAST(dtCadastro AS DATETIME) AS data_cadastro,
CAST(CAST(matricCadastrador AS FLOAT64) AS INT64) AS matricula_cadastrador,
Bairro AS bairro,
UF AS uf,
CAST(CAST(codLogra  AS FLOAT64) AS INT64) AS id_logradouro,
descLogra AS nome_logradouro_fora,
numero AS numero,
compend AS complememto_endereco,
municipio AS municipio,
inscmunicipal AS inscricao_municipal,

FROM {{ source('adm_licenca_urbanismo_staging', 'requerente_processo_documento') }}
