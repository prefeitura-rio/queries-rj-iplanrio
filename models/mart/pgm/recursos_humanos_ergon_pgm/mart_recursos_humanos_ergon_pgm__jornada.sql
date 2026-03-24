{{
    config(
        alias='jornada',
        materialized="table",
        tags=["mart", "ergon", "jornada"],
        description="Tipos de jornadas existentes tanto na administração direta como indireta da prefeitura do Rio de Janeiro."
    )
}}

SELECT
    sigla,
    nome,
    horas_semana,
    horas_mes
FROM {{ ref('raw_recursos_humanos_ergon__jornada') }} AS t


