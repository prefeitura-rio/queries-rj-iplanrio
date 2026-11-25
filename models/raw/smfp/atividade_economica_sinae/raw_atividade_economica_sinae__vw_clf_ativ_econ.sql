{{ config(alias='vw_clf_ativ_econ', materialized='table') }}

SELECT
    CAST(cod_grupo AS STRING) AS cod_grupo,
    CAST(desc_grupo AS STRING) AS desc_grupo,
    CAST(cod_sub_grupo AS STRING) AS cod_sub_grupo,
    CAST(desc_sub_grupo AS STRING) AS desc_sub_grupo,
    CAST(cod_atividade AS STRING) AS cod_atividade,
    CAST(desc_atividade AS STRING) AS desc_atividade,
    CAST(status AS STRING) AS status,
    CAST(divisao AS STRING) AS divisao
FROM {{ source('atividade_economica_sinae_staging', 'vw_clf_ativ_econ') }}
