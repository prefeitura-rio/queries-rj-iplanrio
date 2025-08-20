{{
    config(
        alias='administracao_unidade',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(TRIM(cod_unidade) AS STRING) AS cod_unidade,
  SAFE_CAST(TRIM(cnes) AS STRING) AS cnes,
  SAFE_CAST(TRIM(nome_fantasia) AS STRING) AS nome_fantasia,
  SAFE_CAST(TRIM(sigla_tipo) AS STRING) AS sigla_tipo,
  SAFE_CAST(TRIM(sigla_perfil) AS STRING) AS sigla_perfil,
  SAFE_CAST(TRIM(sigla_gestao) AS STRING) AS sigla_gestao,
  SAFE_CAST(TRIM(sigla_tipo_gestao) AS STRING) AS sigla_tipo_gestao,
  SAFE_CAST(TRIM(razao_social) AS STRING) AS razao_social,
  SAFE_CAST(TRIM(nome_fantasia_original) AS STRING) AS nome_fantasia_original,
  SAFE_CAST(TRIM(unidade_abreviado) AS STRING) AS unidade_abreviado,
  SAFE_CAST(TRIM(sigla) AS STRING) AS sigla,
  SAFE_CAST(TRIM(cnpj) AS STRING) AS cnpj,
  SAFE_CAST(TRIM(endereco) AS STRING) AS endereco,
  SAFE_CAST(TRIM(numero) AS STRING) AS numero,
  SAFE_CAST(TRIM(complemento) AS STRING) AS complemento,
  SAFE_CAST(TRIM(bairro) AS STRING) AS bairro,
  SAFE_CAST(TRIM(municipio) AS STRING) AS municipio,
  SAFE_CAST(TRIM(cod_municipio) AS STRING) AS cod_municipio,
  SAFE_CAST(TRIM(uf) AS STRING) AS uf,
  SAFE_CAST(TRIM(cep) AS STRING) AS cep,
  SAFE_CAST(TRIM(referencia) AS STRING) AS referencia,
  SAFE_CAST(TRIM(telelefone_ddd) AS STRING) AS telefone_ddd,
  SAFE_CAST(TRIM(telefone_1) AS STRING) AS telefone_1,
  SAFE_CAST(TRIM(telefone_1_ramal) AS STRING) AS telefone_1_ramal,
  SAFE_CAST(TRIM(telefone_2) AS STRING) AS telefone_2,
  SAFE_CAST(TRIM(telefone_2_ramal) AS STRING) AS telefone_2_ramal,
  SAFE_CAST(TRIM(fax) AS STRING) AS fax,
  SAFE_CAST(TRIM(telefone_sms_consulta) AS STRING) AS telefone_sms_consulta,
  SAFE_CAST(TRIM(telefone_sms_exame) AS STRING) AS telefone_sms_exame,
  SAFE_CAST(TRIM(email) AS STRING) AS email,
  SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', data_ultima_atualizacao) AS DATETIME) AS data_ultima_atualizacao,
  SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', data_avaliacao) AS DATETIME) AS data_avaliacao
FROM {{ source('brutos_osinfo_staging', 'administracao_unidade') }}