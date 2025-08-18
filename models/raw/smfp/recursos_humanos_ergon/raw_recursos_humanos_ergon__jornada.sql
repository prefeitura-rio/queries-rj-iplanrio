{{
    config(
        alias='jornada',
    )
}}

SELECT
    SAFE_CAST(sigla AS STRING) AS sigla,
    SAFE_CAST(nome AS STRING) AS nome,
    SAFE_CAST(horassem AS STRING) AS horas_semana,
    SAFE_CAST(horasmen AS STRING) AS horas_mes,
FROM {{ source('recursos_humanos_ergon', 'jornada') }} AS t