{{
  config(
    materialized='table',
    alias='bairro'
  )
}}

SELECT
CAST(codBairro AS STRING) AS id_bairro,
CAST(nomBairro AS STRING) AS nome_bairro,
CAST(codRA AS STRING) AS id_regiao_administrativa,
CAST(codOrgaoSMU AS STRING) AS id_orgao_smu,
FROM {{ source('adm_licenca_urbanismo_staging', 'bairro') }}
