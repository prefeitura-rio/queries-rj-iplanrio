{{
    config(
        alias='bem_patrimoniado_tipo',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  {{ clean_and_cast('id_bem_tipo', 'int64', trim=true) }} AS id_bem_tipo,
  SAFE_CAST(TRIM(bem_tipo) AS STRING) AS bem_tipo
FROM {{ source('brutos_osinfo_staging', 'bem_patrimoniado_tipo') }} AS t
