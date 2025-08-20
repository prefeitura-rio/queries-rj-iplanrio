{{
    config(
        alias="obras_suspensas",
        schema='infraestrutura_siscob_obras',
        materialized="table",
    )
}}

SELECT
    DISTINCT
        *
FROM {{ source('brutos_siscob_staging', 'obras_suspensas') }}