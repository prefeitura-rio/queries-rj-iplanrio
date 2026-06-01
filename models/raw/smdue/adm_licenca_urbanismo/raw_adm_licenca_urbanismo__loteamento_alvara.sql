{{
  config(
    materialized='table',
    alias='loteamento_alvara'
  )
}}

SELECT
num_lic AS id_licenciamento,
lotesvinculados AS descricao_lotes_vinculados,
CAST(comprlograprojetados AS FLOAT64) AS comprimento_logradouros_projetados,
CAST(CAST(totallotesprimitivosremembramento AS FLOAT64) AS INT64) AS total_lotes_primitivos_remembramento,
CAST(CAST(totallotesremembrados AS FLOAT64) AS INT64) AS total_lotes_remembrados,
CAST(CAST(totallotesprimitivosdesmembramento AS FLOAT64) AS INT64) AS total_lotes_primitivos_desmembramento,
CAST(CAST(totallotesdesmembrados AS FLOAT64) AS INT64) AS total_lotes_desmembrados,
CAST(CAST(totallotesprimitivosredesmembramento AS FLOAT64) AS INT64) AS total_lotes_primitivos_remembramento_desmembramento,
CAST(CAST(totallotesredesmembrados AS FLOAT64) AS INT64) AS total_lotes_remembramento_desmembramento,
CAST(CAST(totalareasprivativasprimitivas AS FLOAT64) AS INT64) AS total_areas_privativas_primitivas,
CAST(totalareasprivativasresultantes AS FLOAT64) AS total_areas_privativas_resultantes,

FROM {{ source('adm_licenca_urbanismo_staging', 'loteamento_alvara') }}
