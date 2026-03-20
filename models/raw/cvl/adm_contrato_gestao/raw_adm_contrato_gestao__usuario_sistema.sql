{{
    config(
        alias='usuario_sistema',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  {{ clean_and_cast('id_usuario_sistema', 'string', trim=true) }} AS id_usuario_sistema,
  SAFE_CAST(TRIM(cod_usuario) AS STRING) AS cod_usuario,
  {{ clean_and_cast('id_sistema', 'string', trim=true) }} AS id_sistema,
  {{ clean_and_cast('id_perfil', 'string', trim=true) }} AS id_perfil,
  SAFE_CAST(DATE(data_inicial) AS DATE) AS data_inicial,
  SAFE_CAST(DATE(data_final) AS DATE) AS data_final
FROM {{ source('brutos_osinfo_staging', 'usuario_sistema') }} AS t