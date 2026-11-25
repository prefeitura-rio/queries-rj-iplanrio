{{ config(alias='vw_codigo_receita_smf', materialized='table') }}

SELECT
    CAST(codigo AS STRING) AS codigo,
    CAST(descricao AS STRING) AS descricao
FROM {{ source('atividade_economica_sinae_staging', 'vw_codigo_receita_smf') }}
