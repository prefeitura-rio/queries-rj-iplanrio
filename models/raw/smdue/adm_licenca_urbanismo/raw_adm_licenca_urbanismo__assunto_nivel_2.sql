{{
  config(
    materialized='table',
    alias='adm_licenca_urbanismo__assunto_nivel_2'
  )
}}

SELECT
descNivel2 AS assunto_nivel_2,
codAssuntoN2 AS id_assunto_nivel_2,

FROM {{ source('adm_licenca_urbanismo_staging', 'assunto_nivel_2') }}
