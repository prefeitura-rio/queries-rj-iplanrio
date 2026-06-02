{{
  config(
    materialized='table',
    alias='despacho'
  )
}}

SELECT
codTramite AS id_tramite,
codParecer AS id_parecer,
descDespacho AS texto_despacho,

FROM {{ source('adm_licenca_urbanismo_staging', 'despacho') }}
