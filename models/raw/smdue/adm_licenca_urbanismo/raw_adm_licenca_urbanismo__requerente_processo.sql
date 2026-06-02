{{
  config(
    materialized='table',
    alias='requerente_processo'
  )
}}

SELECT
    codDocumento AS id_documento,
    codPessoa AS id_requerente,
    matricCadastrador AS matricula_cadastrador,
    CAST(dtCadastro AS DATETIME) AS data_cadastro

FROM {{ source('adm_licenca_urbanismo_staging', 'requerente_processo') }}
