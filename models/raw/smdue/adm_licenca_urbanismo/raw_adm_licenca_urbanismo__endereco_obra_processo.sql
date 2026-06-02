{{
  config(
    materialized='table',
    alias='endereco_obra_processo'
  )
}}

SELECT
*

FROM {{ source('adm_licenca_urbanismo_staging', 'endereco_obra_processo') }}
