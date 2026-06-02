{{
  config(
    materialized='table',
    alias='tipo_inscricao_imovel'
  )
}}

SELECT
    ID_tpInscricaoImovel AS id_origem_inscricao_imovel,
    TpInscricaoImovel AS origem,

FROM {{ source('adm_licenca_urbanismo_staging', 'tipo_inscricao_imovel') }}
