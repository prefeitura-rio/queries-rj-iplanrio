{{ config(alias='vw_clf_arrecad', materialized='table') }}

SELECT
    CAST(inscmunic AS STRING) AS inscmunic,
    CAST(numguia AS STRING) AS numguia,
    CAST(competencia AS STRING) AS competencia,
    CAST(codreceita AS STRING) AS codreceita,
    CAST(dtvenc AS STRING) AS dtvenc,
    CAST(dtpgto AS STRING) AS dtpgto,
    CAST(tributo AS STRING) AS tributo,
    CAST(vlpago AS STRING) AS vlpago,
    CAST(mora AS STRING) AS mora,
    CAST(multa AS STRING) AS multa,
    CAST(banco AS STRING) AS banco,
    CAST(processo AS STRING) AS processo
FROM {{ source('atividade_economica_sinae_staging', 'vw_clf_arrecad') }}
