{{
    config(
        alias="service_attribute_library",
        materialized="table"
    )
}}
{{ raw_czrm__service_attribute_library("czrm_salesforce_org_staging") }}
