{{ config(alias='vw_clf_ativ_estabelecimento', materialized='table') }}

SELECT
    CAST(inscmunic AS STRING) AS inscmunic,
    CAST(codatividade AS STRING) AS codatividade,
    CAST(seq AS STRING) AS seq
FROM {{ source('atividade_economica_sinae_staging', 'vw_clf_ativ_estabelecimento') }}
