{{
  config(
    materialized='table',
    alias='processo_irph'
  )
}}

SELECT
    ID_IRPH_DOCTO AS id_irph_documento,
    CODDOCUMENTO AS id_documento,
    DESCRICAO AS descricao,

FROM {{ source('adm_licenca_urbanismo_staging', 'processo_irph') }}
