{{
    config(
        alias="service_catalog",
        materialized="table"
    )
}}
{{ raw_czrm__service_catalog("czrm_salesforce_org_producao") }}
