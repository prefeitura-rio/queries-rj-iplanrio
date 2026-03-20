{{
    config(
        alias='conta_bancaria_tipo',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  {{ clean_and_cast('id_conta_bancaria_tipo', 'string', trim=true) }} AS id_conta_bancaria_tipo,
  SAFE_CAST(TRIM(tipo) AS STRING) AS tipo,
  SAFE_CAST(TRIM(sigla) AS STRING) AS sigla
FROM {{ source('brutos_osinfo_staging', 'conta_bancaria_tipo') }} AS t