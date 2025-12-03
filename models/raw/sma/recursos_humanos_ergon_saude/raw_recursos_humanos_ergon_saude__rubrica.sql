{{
    config(
        alias='rubrica_ergon',
    )
}}

SELECT *
FROM {{ source('brutos_ergon_saude_staging', 'RUBRICAS') }} AS t