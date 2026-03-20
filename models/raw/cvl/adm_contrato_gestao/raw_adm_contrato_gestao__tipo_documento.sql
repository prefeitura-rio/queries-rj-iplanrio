{{
    config(
        alias='tipo_documento',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  {{ clean_and_cast('id_tipo_documento', 'string', trim=true) }} AS id_tipo_documento,
  SAFE_CAST(TRIM(tipo_documento) AS STRING) AS tipo_documento,
  SAFE_CAST(TRIM(documento) AS STRING) AS documento
FROM {{ source('brutos_osinfo_staging', 'tipo_documento') }} AS t