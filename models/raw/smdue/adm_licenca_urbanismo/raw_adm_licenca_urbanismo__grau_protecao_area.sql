{{
  config(
    materialized='table',
    alias='grau_protecao_area'
  )
}}

SELECT
    ID_GRAU_PROTECAO AS id_grau_protecao,
    DS_GRAU_PROTECAO AS nome_grau_protecao,

FROM {{ source('adm_licenca_urbanismo_staging', 'grau_protecao_area') }}
