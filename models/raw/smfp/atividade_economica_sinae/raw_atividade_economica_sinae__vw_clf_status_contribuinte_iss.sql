{{ config(alias='vw_clf_status_contribuinte_iss', materialized='table') }}

SELECT
    CAST(codigo AS STRING) AS codigo,
    CAST(descricao AS STRING) AS descricao,
    CAST(status AS STRING) AS status
FROM {{ source('atividade_economica_sinae_staging', 'vw_clf_status_contribuinte_iss') }}
