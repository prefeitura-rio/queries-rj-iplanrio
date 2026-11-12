{{
    config(
        alias="record_type",
        materialized="table"
    )
}}
{{ raw_czrm__record_type("czrm_salesforce_org_staging") }}
