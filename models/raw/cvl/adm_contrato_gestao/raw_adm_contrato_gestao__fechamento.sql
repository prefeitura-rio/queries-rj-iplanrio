{{
    config(
        alias='fechamento',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_fechamento AS STRING)), r'\.0$', '') AS STRING) AS id_fechamento,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(mes_referencia AS STRING)), r'\.0$', '') AS INT64) AS mes_referencia,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(ano_referencia AS STRING)), r'\.0$', '') AS INT64) AS ano_referencia,
  SAFE_CAST(TRIM(SAFE_CAST(cod_usuario AS STRING)) AS STRING) AS cod_usuario,
  SAFE_CAST(data_inclusao AS DATETIME) AS data_inclusao,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_contrato AS STRING)), r'\.0$', '') AS STRING) AS id_instrumento_contratual,
  SAFE_CAST(data_limite AS DATETIME) AS data_limite,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(cod_organizacao AS STRING)), r'\.0$', '') AS INT64) AS cod_instituicao,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(cod_estado_entrega AS STRING)), r'\.0$', '') AS INT64) AS cod_estado_entrega,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(cod_tipo_entrega AS STRING)), r'\.0$', '') AS INT64) AS cod_tipo_entrega
FROM {{ source('brutos_osinfo_staging', 'fechamento') }} AS t
