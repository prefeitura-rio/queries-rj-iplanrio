{{
    config(
        alias="contact",
        materialized="table"
    )
}}
{{ raw_czrm__contact("czrm_salesforce_org_staging") }}
