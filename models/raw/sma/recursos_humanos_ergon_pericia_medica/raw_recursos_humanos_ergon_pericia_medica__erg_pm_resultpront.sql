{{
    config(
        alias='erg_pm_resultpront',
        schema='brutos_ergon_staging'
    )
}}

SELECT
    *
FROM {{ source('recursos_humanos_ergon_pericia_medica_staging', 'erg_pm_resultpront') }}

