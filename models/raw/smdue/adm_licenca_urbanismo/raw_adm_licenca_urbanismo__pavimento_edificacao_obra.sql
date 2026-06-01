{{
  config(
    materialized='table',
    alias='pavimento_edificacao_obra'
  )
}}

SELECT
id_pav AS id_pavimento,
num_lic AS id_licenciamento,
id_edif AS id_edificacao,
pavimento AS pavimento,
CAST(CAST(tot_pav AS FLOAT64) AS INT64) AS total_pavimentos,
CAST(CAST(tot_vagascobertas AS FLOAT64) AS INT64) AS total_vagas_cobertas,
CAST(CAST(tot_vagasdescobertas AS FLOAT64) AS INT64) AS total_vagas_descobertas,

FROM {{ source('adm_licenca_urbanismo_staging', 'pavimento_edificacao_obra') }}
