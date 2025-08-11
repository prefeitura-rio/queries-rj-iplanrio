{{
    config(
        alias='horario_trabalho',
    )
}}

SELECT
    SAFE_CAST(REGEXP_REPLACE(codigo, r'\.0$', '') AS STRING) AS id_horario,
    SAFE_CAST(descricao AS STRING) AS descricao,
    SAFE_CAST(pontlei AS STRING) AS publicacao_diario_oficial,
    SAFE_CAST(carga_hr_mes AS STRING) AS carga_horaria_mes,
    SAFE_CAST(carga_hr_semana AS STRING) AS carga_horaria_semana,
    SAFE_CAST(carga_hr_dia AS STRING) AS carga_horaria_dia,
    SAFE_CAST(REGEXP_REPLACE(id_reg, r'\.0$', '') AS STRING) AS id_registro,
FROM {{ source('recursos_humanos_ergon', 'horario_trabalho') }} AS t