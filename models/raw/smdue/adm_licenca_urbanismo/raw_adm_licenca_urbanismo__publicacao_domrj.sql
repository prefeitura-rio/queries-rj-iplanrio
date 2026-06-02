{{
  config(
    materialized='table',
    alias='publicacao_domrj'
  )
}}

SELECT
codpublicacao AS id_publicacao,
CAST(dtenvio AS DATETIME) AS data_envio,
CAST(dtpublicacao AS DATETIME) AS data_publicacao,
CAST(dtprevista AS DATETIME) AS data_prevista,

FROM {{ source('adm_licenca_urbanismo_staging', 'publicacao_domrj') }}
