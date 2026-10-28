{{
    config(
        alias='tipo_documento',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_tipo_documento AS STRING)), r'\.0$', '') AS STRING) AS id_tipo_documento,
  SAFE_CAST(TRIM(SAFE_CAST(tipo_documento AS STRING)) AS STRING) AS tipo_documento,
  SAFE_CAST(TRIM(SAFE_CAST(documento AS STRING)) AS STRING) AS documento
FROM {{ source('brutos_osinfo_staging', 'tipo_documento') }} AS t
