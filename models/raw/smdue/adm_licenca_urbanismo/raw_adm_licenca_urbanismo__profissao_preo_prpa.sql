{{
  config(
    materialized='table',
    alias='profissao_preo_prpa'
  )
}}

SELECT
ID_PROFISSAO AS id_profissao,
DS_PROFISSAO AS nome_profissao,

FROM {{ source('adm_licenca_urbanismo_staging', 'profissao_preo_prpa') }}
