{{
    config(
        alias='exames',
            )
}}

SELECT
    *
FROM {{ source('brutos_recursos_humanos_ergon_pericia_medica_staging', 'EXAMES') }}

