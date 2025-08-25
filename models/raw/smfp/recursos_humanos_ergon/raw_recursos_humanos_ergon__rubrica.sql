{{
    config(
        alias='ergon_rubrica',
    )
}}

SELECT *
FROM {{ source('recursos_humanos_ergon', 'rubrica') }} AS t