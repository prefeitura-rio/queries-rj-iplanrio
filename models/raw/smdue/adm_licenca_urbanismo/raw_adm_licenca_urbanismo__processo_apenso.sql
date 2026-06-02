{{
  config(
    materialized='table',
    alias='processo_apenso'
  )
}}

SELECT
codDocumentoPrincipal AS id_documento_principal,
codDocumentoApenso AS id_documento_apenso,
MatricCadastrador AS matricula_cadastrador,

FROM {{ source('adm_licenca_urbanismo_staging', 'processo_apenso') }}
