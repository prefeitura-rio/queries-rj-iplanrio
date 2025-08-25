{{
    config(
        alias="enderecos",
        schema="crm_dados_mestres",
        materialized="table",
        tags=["daily"]
    )
}}

select
    DISTINCT *
from {{ source("rj-crm-registy-intermediario-dados-mestres-staging", "enderecos_geolocalizados") }}