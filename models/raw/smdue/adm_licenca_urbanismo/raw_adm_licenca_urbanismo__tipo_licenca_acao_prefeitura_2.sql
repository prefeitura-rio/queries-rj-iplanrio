{{
  config(
    materialized='table',
    alias='tipo_licenca_acao_prefeitura_2'
  )
}}

SELECT
    cod_tipo_lic AS id_tipo_licenca,
    descr_tipo_lic AS nome_tipo_licenca,

FROM {{ source('adm_licenca_urbanismo_staging', 'tipo_licenca_acao_prefeitura_2') }}
