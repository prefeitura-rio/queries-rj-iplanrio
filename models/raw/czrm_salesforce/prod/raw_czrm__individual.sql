{{
    config(
        alias="individual",
        materialized="table"
    )
}}
{{ raw_czrm__individual("czrm_salesforce_org_producao") }}
