{{
    config(
        alias='bem_patrimoniado',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_bem), r'\.0$', '') AS INT64) AS id_bem,
  SAFE_CAST(TRIM(cod_os) AS INT64) AS id_os,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_contrato), r'\.0$', '') AS INT64) AS id_contrato,
  SAFE_CAST(TRIM(ref_mes) AS INT64) AS ref_mes,
  SAFE_CAST(TRIM(ref_ano) AS INT64) AS ref_ano,
  SAFE_CAST(TRIM(id_bem_tipo) AS INT64) AS id_bem_tipo,
  SAFE_CAST(TRIM(descricao) AS STRING) AS descricao,
  SAFE_CAST(TRIM(quantidade) AS NUMERIC) AS quantidade,
  SAFE_CAST(TRIM(nf) AS STRING) AS nf,
  SAFE_CAST(DATE(data_aquisicao) AS DATE) AS data_aquisicao,
  SAFE_CAST(TRIM(vida_util) AS STRING) AS vida_util,
  SAFE_CAST(TRIM(valor) AS NUMERIC) AS valor,
  SAFE_CAST(TRIM(cod_unidade) AS INT64) AS id_unidade,
  SAFE_CAST(TRIM(vinculacao) AS STRING) AS vinculacao,
  SAFE_CAST(TRIM(fornecedor) AS STRING) AS fornecedor,
  SAFE_CAST(TRIM(cnpj) AS STRING) AS cnpj,
  SAFE_CAST(TRIM(img_nf) AS STRING) AS imagem_nf,
  SAFE_CAST(TRIM(setor_destino) AS STRING) AS setor_destino,
  SAFE_CAST(TRIM(id_imagem) AS INT64) AS id_imagem
FROM {{ source('brutos_osinfo_staging', 'bem_patrimoniado') }} AS t
