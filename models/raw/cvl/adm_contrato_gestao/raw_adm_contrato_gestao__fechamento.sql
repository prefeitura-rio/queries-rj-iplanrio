{{
    config(
        alias='fechamento',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_fechamento), r'\.0$', '') AS STRING) AS id_fechamento,
  SAFE_CAST(REGEXP_REPLACE(TRIM(mes_referencia), r'\.0$', '') AS INT64) AS mes_referencia,
  SAFE_CAST(REGEXP_REPLACE(TRIM(ano_referencia), r'\.0$', '') AS INT64) AS ano_referencia,
  SAFE_CAST(TRIM(cod_usuario) AS STRING) AS cod_usuario,
  SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', data_inclusao) AS DATETIME) AS data_inclusao,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_contrato), r'\.0$', '') AS STRING) AS id_instrumento_contratual,
  SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', data_limite) AS DATETIME) AS data_limite,
  SAFE_CAST(REGEXP_REPLACE(TRIM(cod_organizacao), r'\.0$', '') AS INT64) AS cod_instituicao,
  SAFE_CAST(REGEXP_REPLACE(TRIM(cod_estado_entrega), r'\.0$', '') AS INT64) AS cod_estado_entrega,
  SAFE_CAST(REGEXP_REPLACE(TRIM(cod_tipo_entrega), r'\.0$', '') AS INT64) AS cod_tipo_entrega
FROM {{ source('brutos_osinfo_staging', 'fechamento') }} AS t