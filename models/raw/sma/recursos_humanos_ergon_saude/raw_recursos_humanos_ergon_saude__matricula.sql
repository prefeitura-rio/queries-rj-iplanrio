{{
    config(
        alias='matricula',
    )
}}

SELECT
    SAFE_CAST(NUMFUNC AS STRING) AS id_funcionario,
    SAFE_CAST(MATRIC AS STRING) AS id_matricula,
FROM {{ source('brutos_ergon_saude_staging', 'VW_DLK_ERG_ERG_MATRICULAS') }} AS t