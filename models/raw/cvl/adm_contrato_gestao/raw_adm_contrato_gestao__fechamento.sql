{{
    config(
        alias='fechamento',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  {{ clean_and_cast('id_fechamento', 'string', trim=true) }} AS id_fechamento,
  {{ clean_and_cast('mes_referencia', 'int64', trim=true) }} AS mes_referencia,
  {{ clean_and_cast('ano_referencia', 'int64', trim=true) }} AS ano_referencia,
  SAFE_CAST(TRIM(cod_usuario) AS STRING) AS cod_usuario,
  SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', data_inclusao) AS DATETIME) AS data_inclusao,
  {{ clean_and_cast('id_contrato', 'string', trim=true) }} AS id_instrumento_contratual,
  SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', data_limite) AS DATETIME) AS data_limite,
  {{ clean_and_cast('cod_organizacao', 'int64', trim=true) }} AS cod_instituicao,
  {{ clean_and_cast('cod_estado_entrega', 'int64', trim=true) }} AS cod_estado_entrega,
  {{ clean_and_cast('cod_tipo_entrega', 'int64', trim=true) }} AS cod_tipo_entrega
FROM {{ source('brutos_osinfo_staging', 'fechamento') }} AS t