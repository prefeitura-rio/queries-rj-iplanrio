{{
    config(
        alias='historico_alteracoes',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}


SELECT
  {{ clean_and_cast('id_historico_alteracoes', 'string', trim=true) }} AS id_historico_alteracoes,
  {{ clean_and_cast('id_tipo_arquivo', 'string', trim=true) }} AS id_tipo_arquivo,
  SAFE_CAST(TRIM(cod_usuario) AS STRING) AS cod_usuario,
  {{ clean_and_cast('cod_organizacao', 'int64', trim=true) }} AS cod_organizacao,
  SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', data_modificacao) AS DATETIME) AS data_modificacao,
  SAFE_CAST(TRIM(valor_anterior) AS STRING) AS valor_anterior,
  SAFE_CAST(TRIM(valor_novo) AS STRING) AS valor_novo,
  {{ clean_and_cast('mes_referencia', 'int64', trim=true) }} AS mes_referencia,
  {{ clean_and_cast('ano_referencia', 'int64', trim=true) }} AS ano_referencia,
  {{ clean_and_cast('id_registro', 'string', trim=true) }} AS id_registro,
  SAFE_CAST(TRIM(tipo_alteracao) AS STRING) AS tipo_alteracao
FROM {{ source('brutos_osinfo_staging', 'historico_alteracoes') }} AS t