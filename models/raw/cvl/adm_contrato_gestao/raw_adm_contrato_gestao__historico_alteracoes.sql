{{
    config(
        alias='historico_alteracoes',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}


SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_historico_alteracoes), r'\.0$', '') AS STRING) AS id_historico_alteracoes,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_tipo_arquivo), r'\.0$', '') AS STRING) AS id_tipo_arquivo,
  SAFE_CAST(TRIM(cod_usuario) AS STRING) AS cod_usuario,
  SAFE_CAST(REGEXP_REPLACE(TRIM(cod_organizacao), r'\.0$', '') AS INT64) AS cod_organizacao,
  SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', data_modificacao) AS DATETIME) AS data_modificacao,
  SAFE_CAST(TRIM(valor_anterior) AS STRING) AS valor_anterior,
  SAFE_CAST(TRIM(valor_novo) AS STRING) AS valor_novo,
  SAFE_CAST(REGEXP_REPLACE(TRIM(mes_referencia), r'\.0$', '') AS INT64) AS mes_referencia,
  SAFE_CAST(REGEXP_REPLACE(TRIM(ano_referencia), r'\.0$', '') AS INT64) AS ano_referencia,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_registro), r'\.0$', '') AS STRING) AS id_registro,
  SAFE_CAST(TRIM(tipo_alteracao) AS STRING) AS tipo_alteracao
FROM {{ source('brutos_osinfo_staging', 'historico_alteracoes') }} AS t