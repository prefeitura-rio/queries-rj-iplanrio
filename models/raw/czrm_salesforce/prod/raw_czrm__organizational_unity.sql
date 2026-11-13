{{
    config(
        alias="organizational_unity",
        materialized="table"
    )
}}
{{ raw_czrm__organizational_unity("czrm_salesforce_org_producao") }}
