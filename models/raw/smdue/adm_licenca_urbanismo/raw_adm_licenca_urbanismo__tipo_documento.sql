{{
  config(
    materialized='table',
    alias='tipo_documento'
  )
}}

SELECT
codTipoDocumento AS id_tipo_documento,
descTipoDocumento AS nome_tipo_documento,

FROM {{ source('adm_licenca_urbanismo_staging', 'tipo_documento') }}
