{{
    config(
        schema="brutos_gcp",
        alias="gcp_billing",
        materialized="view")
}}

select * from  `dados-rio-billing.billing.gcp_billing_export_*`