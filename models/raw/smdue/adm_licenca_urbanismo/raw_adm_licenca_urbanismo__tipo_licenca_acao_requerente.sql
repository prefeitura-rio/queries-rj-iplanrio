{{
  config(
    materialized='table',
    alias='tipo_licenca_acao_requerente'
  )
}}

SELECT
    *

FROM {{ source('adm_licenca_urbanismo_staging', 'tipo_licenca_acao_requerente') }}
