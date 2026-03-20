{{
    config(
        alias='administracao_perfil',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  {{ clean_and_cast('id_perfil', 'string', trim=true) }} AS id_perfil,
  SAFE_CAST(TRIM(nome_perfil) AS STRING) AS nome_perfil
FROM {{ source('brutos_osinfo_staging', 'administracao_perfil') }} AS t