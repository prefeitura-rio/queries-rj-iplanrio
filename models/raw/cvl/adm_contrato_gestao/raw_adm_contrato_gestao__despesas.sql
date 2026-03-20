{{
    config(
        alias='despesas',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  {{ clean_and_cast('id_documento', 'string', trim=true) }} AS id_documento,
  SAFE_CAST(TRIM(cod_organizacao) AS STRING) AS cod_organizacao,
  SAFE_CAST(TRIM(cod_unidade) AS STRING) AS cod_unidade,
  SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', data_envio) AS DATETIME) AS data_envio,
  {{ clean_and_cast('id_tipo_documento', 'string', trim=true) }} AS id_tipo_documento,
  SAFE_CAST(TRIM(codigo_fiscal) AS STRING) AS codigo_fiscal,
  SAFE_CAST(TRIM(cnpj) AS STRING) AS cnpj,
  SAFE_CAST(TRIM(razao) AS STRING) AS razao,
  SAFE_CAST(TRIM(cpf) AS STRING) AS cpf,
  SAFE_CAST(TRIM(nome) AS STRING) AS nome,
  SAFE_CAST(TRIM(num_documento) AS STRING) AS num_documento,
  SAFE_CAST(TRIM(serie) AS STRING) AS serie,
  SAFE_CAST(TRIM(descricao) AS STRING) AS descricao,
  SAFE_CAST(DATE(data_emissao) AS DATE) AS data_emissao,
  SAFE_CAST(DATE(data_vencimento) AS DATE) AS data_vencimento,
  SAFE_CAST(DATE(data_pagamento) AS DATE) AS data_pagamento,
  SAFE_CAST(data_apuracao AS DATE) AS data_apuracao,
  SAFE_CAST(TRIM(valor_documento) AS NUMERIC) AS valor_documento,
  SAFE_CAST(TRIM(valor_pago) AS NUMERIC) AS valor_pago,
  {{ clean_and_cast('id_despesa', 'string', trim=true) }} AS id_despesa,
  {{ clean_and_cast('id_rubrica', 'string', trim=true) }} AS id_rubrica,
  {{ clean_and_cast('id_contrato', 'string', trim=true) }} AS id_contrato,
  {{ clean_and_cast('id_conta_bancaria', 'string', trim=true) }} AS id_conta_bancaria,
  SAFE_CAST(TRIM(referencia_mes) AS STRING) AS referencia_mes,
  SAFE_CAST(TRIM(referencia_ano) AS STRING) AS referencia_ano,
  SAFE_CAST(TRIM(cod_bancario) AS STRING) AS cod_bancario,
  SAFE_CAST(TRIM(flg_justificativa) AS STRING) AS flg_justificativa,
  {{ clean_and_cast('parcelamento_mes', 'int64', trim=true) }} AS parcela_mes,
  {{ clean_and_cast('parcelamento_total', 'int64', trim=true) }} AS parcelamento_total,
  {{ clean_and_cast('id_imagem', 'string', trim=true) }} AS id_imagem,
  SAFE_CAST(TRIM(nf_validada_sigma) AS STRING) AS nf_validada_sigma,
  SAFE_CAST(DATE(data_validacao) AS DATE) AS data_validacao
FROM {{ source('brutos_osinfo_staging', 'despesa') }} AS t