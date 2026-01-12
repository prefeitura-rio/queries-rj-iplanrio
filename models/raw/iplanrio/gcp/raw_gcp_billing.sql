{{
    config(
        alias="gcp_billing",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "invoice_competencia_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}


{% set previous_day = modules.datetime.date.today() - modules.datetime.timedelta(
    days=1
) %}

{% set partition_to_replace = modules.datetime.date(
    previous_day.year, previous_day.month, 1
) %}


with
    source as (select * from `dados-rio-billing.billing.gcp_billing_export_*`),

    final as (

        select
            *,
            safe_cast(
                concat(
                    left(invoice.month, 4), '-', right(invoice.month, 2), '-01'
                ) as date
            ) as invoice_competencia_particao
        from source
    )

select *
from final
{% if is_incremental() %}
    where invoice_competencia_particao = '{{ partition_to_replace }}'
{% endif %}
