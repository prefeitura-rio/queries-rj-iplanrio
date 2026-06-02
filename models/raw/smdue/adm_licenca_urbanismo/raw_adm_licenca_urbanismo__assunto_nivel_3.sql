{{
  config(
    materialized='table',
    alias='assunto_nivel_3'
  )
}}

SELECT
descNivel3 AS assunto_nivel_3,
codAssuntoN3 AS id_assunto_nivel_3,

FROM {{ source('adm_licenca_urbanismo_staging', 'assunto_nivel_3') }}