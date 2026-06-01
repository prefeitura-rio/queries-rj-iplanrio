{{
  config(
    materialized='table',
    alias='licenca_substituida'
  )
}}

SELECT
*

FROM {{ source('adm_licenca_urbanismo_staging', 'licenca_substituida') }}
