{{
  config(
    materialized='table',
    alias='restricao_fase'
  )
}}

SELECT
    Cod_restricao AS id_restricao,
    Cod_fase AS id_fase,
    desc_restricao AS nome_restricao,
    Tipo_Compl AS tipo_compl,
    Compl AS compl,
    CAST(ATIVO AS BOOL) AS ativo,

FROM {{ source('adm_licenca_urbanismo_staging', 'restricao_fase') }}
