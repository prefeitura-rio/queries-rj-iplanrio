{{
    config(
        alias="neighborhood",
        materialized="table"
    )
}}
{{ raw_czrm__neighborhood("czrm_salesforce_org_producao") }}
