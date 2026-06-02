{{
  config(
    materialized='table',
    alias='orgao_licenciamento'
  )
}}

SELECT
    codorgao AS id_orgao_smu,
    codorgaosigma AS id_orgao_sislic,
    CAST(ativo AS BOOL) AS ativo,

FROM {{ source('adm_licenca_urbanismo_staging', 'orgao_licenciamento') }}
