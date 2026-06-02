{{
  config(
    materialized='table',
    alias='tipo_licenca_acao_prefeitura_4'
  )
}}

SELECT
    cod_lic AS id_licenca,
    cod_compl_lic AS id_compl_licenca,
    compl AS nome_complemento,

FROM {{ source('adm_licenca_urbanismo_staging', 'tipo_licenca_acao_prefeitura_4') }}
