{{
    config(
        alias='historico_alteracoes',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}


SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_historico_alteracoes AS STRING)), r'\.0$', '') AS STRING) AS id_historico_alteracoes,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_tipo_arquivo AS STRING)), r'\.0$', '') AS STRING) AS id_tipo_arquivo,
  SAFE_CAST(TRIM(SAFE_CAST(cod_usuario AS STRING)) AS STRING) AS cod_usuario,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(cod_organizacao AS STRING)), r'\.0$', '') AS INT64) AS cod_organizacao,
  SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', data_modificacao) AS DATETIME) AS data_modificacao,
  SAFE_CAST(TRIM(SAFE_CAST(valor_anterior AS STRING)) AS STRING) AS valor_anterior,
  SAFE_CAST(TRIM(SAFE_CAST(valor_novo AS STRING)) AS STRING) AS valor_novo,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(mes_referencia AS STRING)), r'\.0$', '') AS INT64) AS mes_referencia,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(ano_referencia AS STRING)), r'\.0$', '') AS INT64) AS ano_referencia,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_registro AS STRING)), r'\.0$', '') AS STRING) AS id_registro,
  SAFE_CAST(TRIM(SAFE_CAST(tipo_alteracao AS STRING)) AS STRING) AS tipo_alteracao
FROM {{ source('brutos_osinfo_staging', 'historico_alteracoes') }} AS t
