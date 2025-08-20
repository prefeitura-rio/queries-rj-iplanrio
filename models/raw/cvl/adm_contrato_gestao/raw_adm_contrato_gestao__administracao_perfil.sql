{{
    config(
        alias='administracao_perfil',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_perfil), r'\.0$', '') AS STRING) AS id_perfil,
  SAFE_CAST(TRIM(nome_perfil) AS STRING) AS nome_perfil
FROM {{ source('brutos_osinfo_staging', 'administracao_perfil') }}