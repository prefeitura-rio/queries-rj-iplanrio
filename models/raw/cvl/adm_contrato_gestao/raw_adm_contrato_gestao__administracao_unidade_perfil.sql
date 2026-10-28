{{
    config(
        alias='administracao_unidade_perfil',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_unidade_perfil AS STRING)), r'\.0$', '') AS STRING) AS id_unidade_perfil,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_usuario AS STRING)), r'\.0$', '') AS STRING) AS id_usuario,
  SAFE_CAST(TRIM(SAFE_CAST(cod_unidade AS STRING)) AS STRING) AS cod_unidade,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_perfil AS STRING)), r'\.0$', '') AS STRING) AS id_perfil
FROM {{ source('brutos_osinfo_staging', 'administracao_unidade_perfil') }} AS t
