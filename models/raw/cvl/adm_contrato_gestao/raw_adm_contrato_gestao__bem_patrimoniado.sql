{{
    config(
        alias='bem_patrimoniado',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_bem AS STRING)), r'\.0$', '') AS INT64) AS id_bem,
  SAFE_CAST(TRIM(SAFE_CAST(cod_os AS STRING)) AS INT64) AS id_os,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_contrato AS STRING)), r'\.0$', '') AS INT64) AS id_contrato,
  SAFE_CAST(TRIM(SAFE_CAST(ref_mes AS STRING)) AS INT64) AS ref_mes,
  SAFE_CAST(TRIM(SAFE_CAST(ref_ano AS STRING)) AS INT64) AS ref_ano,
  SAFE_CAST(TRIM(SAFE_CAST(id_bem_tipo AS STRING)) AS INT64) AS id_bem_tipo,
  SAFE_CAST(TRIM(SAFE_CAST(descricao AS STRING)) AS STRING) AS descricao,
  SAFE_CAST(TRIM(SAFE_CAST(quantidade AS STRING)) AS NUMERIC) AS quantidade,
  SAFE_CAST(TRIM(SAFE_CAST(nf AS STRING)) AS STRING) AS nf,
  SAFE_CAST(DATE(data_aquisicao) AS DATE) AS data_aquisicao,
  SAFE_CAST(TRIM(SAFE_CAST(vida_util AS STRING)) AS STRING) AS vida_util,
  SAFE_CAST(TRIM(SAFE_CAST(valor AS STRING)) AS NUMERIC) AS valor,
  SAFE_CAST(TRIM(SAFE_CAST(cod_unidade AS STRING)) AS INT64) AS id_unidade,
  SAFE_CAST(TRIM(SAFE_CAST(vinculacao AS STRING)) AS STRING) AS vinculacao,
  SAFE_CAST(TRIM(SAFE_CAST(fornecedor AS STRING)) AS STRING) AS fornecedor,
  SAFE_CAST(TRIM(SAFE_CAST(cnpj AS STRING)) AS STRING) AS cnpj,
  SAFE_CAST(TRIM(SAFE_CAST(img_nf AS STRING)) AS STRING) AS imagem_nf,
  SAFE_CAST(TRIM(SAFE_CAST(setor_destino AS STRING)) AS STRING) AS setor_destino,
  SAFE_CAST(TRIM(SAFE_CAST(id_imagem AS STRING)) AS INT64) AS id_imagem
FROM {{ source('brutos_osinfo_staging', 'bem_patrimoniado') }} AS t
