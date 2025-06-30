{{
    config(
    schema="brutos_gcp",
    alias="gcp_billing",
    materialized="incremental",
    unique_key=["billing_account_id", "service.id", "sku.id", "usage_start_time", "project.id", "location.location"],
    incremental_strategy="merge",
    partition_by={
        "field": "usage_end_time",
        "data_type": "timestamp",
        "granularity": "month"
    },
    tags=["incremental"]
) }}

select *
from `dados-rio-billing.billing.gcp_billing_export_*`
{% if is_incremental() %}
    where usage_end_time > (
        select coalesce(max(usage_end_time), timestamp('1970-01-01'))
        from {{ this }}
    )
{% endif %}
