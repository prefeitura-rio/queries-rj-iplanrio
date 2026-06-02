{{
  config(
    materialized='table',
    alias='subdivisao_edificacao_obra'
  )
}}

SELECT
id_subdivisao AS id_subdivisao,
num_lic AS id_licenciamento,
id_edif AS id_edificacao,
descsubdivisao AS subdivisao,

FROM {{ source('adm_licenca_urbanismo_staging', 'subdivisao_edificacao_obra') }}
