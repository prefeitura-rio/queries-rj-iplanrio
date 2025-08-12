{{
    config(
        alias='rubrica',
    )
}}

SELECT *
FROM {{ source('recursos_humanos_ergon', 'rubrica') }} AS t