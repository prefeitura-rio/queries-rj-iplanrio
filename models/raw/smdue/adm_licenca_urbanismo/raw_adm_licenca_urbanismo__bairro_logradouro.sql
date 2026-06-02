{{
  config(
    materialized='table',
    alias='bairro_logradouro'
  )
}}

SELECT
codLogra AS id_logradouro,
codBairro AS id_bairro,

FROM {{ source('adm_licenca_urbanismo_staging', 'bairro_logradouro') }}
