{{
    config(
        alias='despesas',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_documento AS STRING)), r'\.0$', '') AS STRING) AS id_documento,
  SAFE_CAST(TRIM(SAFE_CAST(cod_organizacao AS STRING)) AS STRING) AS cod_organizacao,
  SAFE_CAST(TRIM(SAFE_CAST(cod_unidade AS STRING)) AS STRING) AS cod_unidade,
  SAFE_CAST(data_envio AS DATETIME) AS data_envio,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_tipo_documento AS STRING)), r'\.0$', '') AS STRING) AS id_tipo_documento,
  SAFE_CAST(TRIM(SAFE_CAST(codigo_fiscal AS STRING)) AS STRING) AS codigo_fiscal,
  SAFE_CAST(TRIM(SAFE_CAST(cnpj AS STRING)) AS STRING) AS cnpj,
  SAFE_CAST(TRIM(SAFE_CAST(razao AS STRING)) AS STRING) AS razao,
  SAFE_CAST(TRIM(SAFE_CAST(cpf AS STRING)) AS STRING) AS cpf,
  SAFE_CAST(TRIM(SAFE_CAST(nome AS STRING)) AS STRING) AS nome,
  SAFE_CAST(TRIM(SAFE_CAST(num_documento AS STRING)) AS STRING) AS num_documento,
  SAFE_CAST(TRIM(SAFE_CAST(serie AS STRING)) AS STRING) AS serie,
  SAFE_CAST(TRIM(SAFE_CAST(descricao AS STRING)) AS STRING) AS descricao,
  SAFE_CAST(data_emissao AS DATE) AS data_emissao,
  SAFE_CAST(data_vencimento AS DATE) AS data_vencimento,
  SAFE_CAST(data_pagamento AS DATE) AS data_pagamento,
  SAFE_CAST(data_apuracao AS DATE) AS data_apuracao,
  SAFE_CAST(TRIM(SAFE_CAST(valor_documento AS STRING)) AS NUMERIC) AS valor_documento,
  SAFE_CAST(TRIM(SAFE_CAST(valor_pago AS STRING)) AS NUMERIC) AS valor_pago,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_despesa AS STRING)), r'\.0$', '') AS STRING) AS id_despesa,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_rubrica AS STRING)), r'\.0$', '') AS STRING) AS id_rubrica,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_contrato AS STRING)), r'\.0$', '') AS STRING) AS id_contrato,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_conta_bancaria AS STRING)), r'\.0$', '') AS STRING) AS id_conta_bancaria,
  SAFE_CAST(TRIM(SAFE_CAST(referencia_mes AS STRING)) AS STRING) AS referencia_mes,
  SAFE_CAST(TRIM(SAFE_CAST(referencia_ano AS STRING)) AS STRING) AS referencia_ano,
  SAFE_CAST(TRIM(SAFE_CAST(cod_bancario AS STRING)) AS STRING) AS cod_bancario,
  SAFE_CAST(TRIM(SAFE_CAST(flg_justificativa AS STRING)) AS STRING) AS flg_justificativa,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(parcelamento_mes AS STRING)), r'\.0$', '') AS INT64) AS parcela_mes,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(parcelamento_total AS STRING)), r'\.0$', '') AS INT64) AS parcelamento_total,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_imagem AS STRING)), r'\.0$', '') AS STRING) AS id_imagem,
  SAFE_CAST(TRIM(SAFE_CAST(nf_validada_sigma AS STRING)) AS STRING) AS nf_validada_sigma,
  SAFE_CAST(data_validacao AS DATE) AS data_validacao
FROM {{ source('brutos_osinfo_staging', 'despesa') }} AS t