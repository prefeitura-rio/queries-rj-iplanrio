{{
    config(
        alias='administracao_unidade_perfil',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  {{ clean_and_cast('id_unidade_perfil', 'string', trim=true) }} AS id_unidade_perfil,
  {{ clean_and_cast('id_usuario', 'string', trim=true) }} AS id_usuario,
  SAFE_CAST(TRIM(cod_unidade) AS STRING) AS cod_unidade,
  {{ clean_and_cast('id_perfil', 'string', trim=true) }} AS id_perfil
FROM {{ source('brutos_osinfo_staging', 'administracao_unidade_perfil') }} AS t