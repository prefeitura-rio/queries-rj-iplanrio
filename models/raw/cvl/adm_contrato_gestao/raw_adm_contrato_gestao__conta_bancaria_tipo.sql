{{
    config(
        alias='conta_bancaria_tipo',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_conta_bancaria_tipo AS STRING)), r'\.0$', '') AS STRING) AS id_conta_bancaria_tipo,
  SAFE_CAST(TRIM(SAFE_CAST(tipo AS STRING)) AS STRING) AS tipo,
  SAFE_CAST(TRIM(SAFE_CAST(sigla AS STRING)) AS STRING) AS sigla
FROM {{ source('brutos_osinfo_staging', 'conta_bancaria_tipo') }} AS t
