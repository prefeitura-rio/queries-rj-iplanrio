{{
    config(
        alias="queue_group",
        materialized="table"
    )
}}
{{ raw_czrm__queue_group("czrm_salesforce_org_producao") }}
