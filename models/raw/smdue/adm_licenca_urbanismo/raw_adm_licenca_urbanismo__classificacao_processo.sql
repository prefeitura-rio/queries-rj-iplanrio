{{
  config(
    materialized='table',
    alias='classificacao_processo'
  )
}}

SELECT
ID_CLASSIFICACAO_PROCESSO AS id_classificacao_processo,
DS_CLASSIFICACAO_PROCESSO AS classificacao_processo,

FROM {{ source('adm_licenca_urbanismo_staging', 'classificacao_processo') }}
