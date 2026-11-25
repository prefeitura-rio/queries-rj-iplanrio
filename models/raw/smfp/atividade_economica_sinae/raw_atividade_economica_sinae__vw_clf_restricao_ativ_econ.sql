{{ config(alias='vw_clf_restricao_ativ_econ', materialized='table') }}

SELECT
    CAST(inscmunic AS STRING) AS inscmunic,
    CAST(codigo AS STRING) AS codigo,
    CAST(descricao AS STRING) AS descricao
FROM {{ source('atividade_economica_sinae_staging', 'vw_clf_restricao_ativ_econ') }}
