{{
    config(
        alias="user",
        materialized="table"
    )
}}
{{ raw_czrm__user("czrm_salesforce_org_producao") }}
