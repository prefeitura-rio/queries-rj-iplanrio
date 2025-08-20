{{
    config(
        alias="fiscal",
        schema='infraestrutura_siscob_obras',
        materialized="table",
    )
}}

select distinct * from {{ source('brutos_siscob_staging', 'fiscal') }}