{{
    config(
        alias='rubrica',
        schema='brutos_recursos_humanos_ergon',
    )
}}

SELECT *
FROM {{ source('recursos_humanos_ergon', 'rubrica') }} AS t