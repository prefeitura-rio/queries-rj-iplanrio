{{
    config(
        alias='jornada',
        materialized="table",
        tags=["raw", "ergon", "jornada"],
        description="Tipos de jornadas existentes tanto na administração direta como indireta da prefeitura do Rio de Janeiro. Jornada é a carga horária semanal."
    )
}}

SELECT
    SAFE_CAST(sigla AS STRING) AS sigla,
    SAFE_CAST(nome AS STRING) AS nome,
    SAFE_CAST(horassem AS numeric) AS horas_semana,
    SAFE_CAST(horasmen AS numeric) AS horas_mes
FROM {{ source('brutos_ergon_staging', 'VW_DLK_ERG_JORNADAS_') }} AS t