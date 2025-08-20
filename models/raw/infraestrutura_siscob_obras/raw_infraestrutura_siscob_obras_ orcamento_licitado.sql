{{
    config(
        alias="orcamento_licitado",
        schema='infraestrutura_siscob_obras',
        materialized="table",
    )
}}

SELECT
    DISTINCT
        *
FROM {{ source('brutos_siscob_staging', 'orcamento_licitado') }}