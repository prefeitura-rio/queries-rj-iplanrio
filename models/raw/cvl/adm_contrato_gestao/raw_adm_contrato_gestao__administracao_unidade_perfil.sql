{{
    config(
        alias='administracao_unidade_perfil',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_unidade_perfil), r'\.0$', '') AS STRING) AS id_unidade_perfil,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_usuario), r'\.0$', '') AS STRING) AS id_usuario,
  SAFE_CAST(TRIM(cod_unidade) AS STRING) AS cod_unidade,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_perfil), r'\.0$', '') AS STRING) AS id_perfil
FROM {{ source('brutos_osinfo_staging', 'administracao_unidade_perfil') }} AS t