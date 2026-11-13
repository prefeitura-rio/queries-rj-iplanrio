{{
    config(
        alias="work_order_attribute_answer",
        materialized="table"
    )
}}
{{ raw_czrm__work_order_attribute_answer("czrm_salesforce_org_staging") }}
