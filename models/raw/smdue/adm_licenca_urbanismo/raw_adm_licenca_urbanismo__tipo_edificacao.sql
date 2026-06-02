{{
  config(
    materialized='table',
    alias='tipo_edificacao'
  )
}}

SELECT
    Cod_TpEdificacao AS id_tipo_edificacao,
    Desc_TpEdificacao AS nome_tipo_edificacao,

FROM {{ source('adm_licenca_urbanismo_staging', 'tipo_edificacao') }}
