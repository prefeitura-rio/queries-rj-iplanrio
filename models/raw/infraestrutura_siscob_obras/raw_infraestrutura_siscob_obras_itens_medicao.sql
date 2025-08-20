{{
    config(
        alias="itens_medicao",
        schema='infraestrutura_siscob_obras',
        materialized="table",
    )
}}


SELECT
    DISTINCT
        *
FROM {{ source('brutos_siscob_staging', 'itens_medicao') }}
