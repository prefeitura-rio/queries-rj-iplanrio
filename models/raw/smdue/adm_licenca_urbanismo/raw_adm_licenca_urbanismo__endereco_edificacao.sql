{{
  config(
    materialized='table',
    alias='endereco_edificacao'
  )
}}

SELECT
id_edif  AS id_edificacao ,
ID_Endereco AS id_endereco,

FROM {{ source('adm_licenca_urbanismo_staging', 'endereco_edificacao') }}
