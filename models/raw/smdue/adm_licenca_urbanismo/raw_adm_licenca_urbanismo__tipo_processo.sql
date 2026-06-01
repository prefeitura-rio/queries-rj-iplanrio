{{
  config(
    materialized='table',
    alias='tipo_processo'
  )
}}

SELECT
ID_TIPO_PROCESSO AS id_tipo_processo,
DS_TIPO_PROCESSO AS tipo_processo,

FROM {{ source('adm_licenca_urbanismo_staging', 'tipo_processo') }}
