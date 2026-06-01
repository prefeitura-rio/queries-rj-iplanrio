{{
  config(
    materialized='table',
    alias='orgao_origem_classificacao'
  )
}}

SELECT
ID_ORIGEM_CLASSIFICACAO_PROCESSO AS id_origem_classificacao_processo,
DS_ORIGEM_CLASSIFICACAO_PROCESSO AS origem_classficacao_processo,

FROM {{ source('adm_licenca_urbanismo_staging', 'orgao_origem_classificacao') }}
