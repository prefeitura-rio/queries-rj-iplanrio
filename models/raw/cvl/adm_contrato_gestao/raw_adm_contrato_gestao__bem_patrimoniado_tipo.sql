{{
    config(
        alias='bem_patrimoniado_tipo',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_bem_tipo), r'\.0$', '') AS INT64) AS id_bem_tipo,
  SAFE_CAST(TRIM(bem_tipo) AS STRING) AS bem_tipo
FROM {{ source('brutos_osinfo_staging', 'bem_patrimoniado_tipo') }} AS t
