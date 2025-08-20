{{
    config(
        alias='despesas',
        schema='brutos_adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_documento), r'\.0$', '') AS STRING) AS id_documento,
  SAFE_CAST(TRIM(cod_organizacao) AS STRING) AS cod_organizacao,
  SAFE_CAST(TRIM(cod_unidade) AS STRING) AS cod_unidade,
  SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', data_envio) AS DATETIME) AS data_envio,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_tipo_documento), r'\.0$', '') AS STRING) AS id_tipo_documento,
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
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_despesa), r'\.0$', '') AS STRING) AS id_despesa,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_rubrica), r'\.0$', '') AS STRING) AS id_rubrica,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_contrato), r'\.0$', '') AS STRING) AS id_contrato,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_conta_bancaria), r'\.0$', '') AS STRING) AS id_conta_bancaria,
  SAFE_CAST(TRIM(referencia_mes) AS STRING) AS referencia_mes,
  SAFE_CAST(TRIM(referencia_ano) AS STRING) AS referencia_ano,
  SAFE_CAST(TRIM(cod_bancario) AS STRING) AS cod_bancario,
  SAFE_CAST(TRIM(flg_justificativa) AS STRING) AS flg_justificativa,
  SAFE_CAST(REGEXP_REPLACE(TRIM(parcelamento_mes), r'\.0$', '') AS INT64) AS parcela_mes,
  SAFE_CAST(REGEXP_REPLACE(TRIM(parcelamento_total), r'\.0$', '') AS INT64) AS parcelamento_total,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_imagem), r'\.0$', '') AS STRING) AS id_imagem,
  SAFE_CAST(TRIM(nf_validada_sigma) AS STRING) AS nf_validada_sigma,
  SAFE_CAST(DATE(data_validacao) AS DATE) AS data_validacao
FROM {{ source('brutos_osinfo_staging', 'despesa') }}