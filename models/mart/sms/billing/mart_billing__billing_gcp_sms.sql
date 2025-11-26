{{
  config(
    alias="billing_gcp_sms",
    materialized="table",
    description="Consolidação de dados de billing do GCP dos ambientes geridos pela Saúde"
  )
}}
with
    source as (
        select * 
        from {{ ref("raw_billing_ncn__billing_gcp") }}
    ),

    billing_sms as (
        select * 
        from source 
        where project_id in (
            'rj-sms',
            'rj-sms-dev'
            'rj-sms-sandbox'
            'rj-ivisa'
        )
    )

select *
from billing_sms