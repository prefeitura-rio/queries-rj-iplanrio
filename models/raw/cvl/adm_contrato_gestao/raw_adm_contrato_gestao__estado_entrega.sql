{{
    config(
        alias='estado_entrega',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(TRIM(SAFE_CAST(cod_estado AS STRING)) AS STRING) AS cod_estado,
  SAFE_CAST(TRIM(SAFE_CAST(estado AS STRING)) AS STRING) AS estado,
  SAFE_CAST(TRIM(SAFE_CAST(etiqueta AS STRING)) AS STRING) AS etiqueta,
  SAFE_CAST(TRIM(SAFE_CAST(detalhe AS STRING)) AS STRING) AS detalhe
FROM {{ source('brutos_osinfo_staging', 'estado_entrega') }} AS t
