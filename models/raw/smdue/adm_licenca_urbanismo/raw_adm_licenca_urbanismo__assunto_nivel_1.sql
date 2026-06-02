{{
  config(
    materialized='table',
    alias='adm_licenca_urbanismo__assunto_nivel_1'
  )
}}

SELECT
descNivel1 AS assunto_nivel_1,
codAssuntoN1 AS id_assunto_nivel_1,

FROM {{ source('adm_licenca_urbanismo_staging', 'assunto_nivel_1') }}
