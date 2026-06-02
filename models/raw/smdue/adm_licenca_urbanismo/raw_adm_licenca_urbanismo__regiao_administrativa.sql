{{
  config(
    materialized='table',
    alias='regiao_administrativa'
  )
}}

SELECT
    codRA AS id_regiao_administrativa,
    nomRA AS identificacao_alg_romano,
    codAP AS id_area_planejamento,

FROM {{ source('adm_licenca_urbanismo_staging', 'regiao_administrativa') }}