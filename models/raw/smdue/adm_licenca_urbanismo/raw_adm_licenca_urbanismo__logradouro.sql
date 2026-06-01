{{
  config(
    materialized='table',
    alias='logradouro'
  )
}}

SELECT
codlogra AS id_logradouro,
CAST(CAST(CAST(codlograsmf AS FLOAT64) AS INT64) AS STRING) AS codigo_logradouro_smf,
codtipologra AS tipo_logradouro,
codnobreza AS codigo_nobreza,
descprepo AS preposicao_logradouro,
nomlogra AS nome_logradouro,

FROM {{ source('adm_licenca_urbanismo_staging', 'logradouro') }}
