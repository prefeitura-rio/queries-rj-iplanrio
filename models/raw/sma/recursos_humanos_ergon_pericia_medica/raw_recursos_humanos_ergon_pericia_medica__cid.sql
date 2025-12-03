{{
    config(
        alias='cid',
        schema='brutos_ergon_staging'
    )
}}

SELECT
    *
FROM {{ source('recursos_humanos_ergon_pericia_medica_staging', 'cid') }}

