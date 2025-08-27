{{
    config(
        alias='ergon_rubrica',
    )
}}

SELECT *
FROM {{ source('brutos_ergon_staging', 'RUBRICAS') }} AS t