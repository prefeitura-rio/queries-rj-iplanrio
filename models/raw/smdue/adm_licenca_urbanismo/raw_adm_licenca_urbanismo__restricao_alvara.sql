{{
  config(
    materialized='table',
    alias='restricao_alvara'
  )
}}

SELECT
CAST(CAST(id_rest AS FLOAT64) AS INT64) AS id_restricao,
CAST(CAST(num_lic AS FLOAT64) AS INT64) AS id_licenciamento,
CAST(CAST(cod_restricao AS FLOAT64) AS INT64) AS id_tipo_restricao,
CAST(compl_restricao AS STRING) AS compl_tipo_restricao,
CAST(outra_restricao AS STRING) AS outra_restricao,
CAST(data_baixa AS DATETIME) AS data_baixa,
CAST(baixa_exofficio AS STRING) AS baixa_exofficio

FROM {{ source('adm_licenca_urbanismo_staging', 'restricao_alvara') }}
