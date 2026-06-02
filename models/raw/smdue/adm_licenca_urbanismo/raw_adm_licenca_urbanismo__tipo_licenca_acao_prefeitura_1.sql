{{
  config(
    materialized='table',
    alias='tipo_licenca_acao_prefeitura_1'
  )
}}

SELECT
    cod_classe AS id_classe,
    descr_classe AS nome_classe,

FROM {{ source('adm_licenca_urbanismo_staging', 'tipo_licenca_acao_prefeitura_1') }}
