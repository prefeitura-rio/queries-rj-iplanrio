{{
    config(
        alias='rubrica_ergon',
    )
}}

SELECT *
FROM {{ source('brutos_ergon_staging', 'RUBRICAS') }} AS t