{{
    config(
        alias='raw_recursos_humanos_ergon__rubrica',
    )
}}

SELECT *
FROM {{ source('recursos_humanos_ergon', 'rubrica') }} AS t