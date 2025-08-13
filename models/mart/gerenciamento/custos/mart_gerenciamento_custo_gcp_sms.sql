{{
    config(
        project = ("rj-sms" if target.name == "prod" else "rj-iplanrio-dev"),
        schema="gerenciamento_custos",
        alias="custo_gcp_sms",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "invoice_competencia_particao",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by=["orgao"],
        tags=["custo_gcp"],
    )
}}


-- Variaveis
{% set previous_day = modules.datetime.date.today() - modules.datetime.timedelta(
    days=1
) %}

{% set partition_to_replace = modules.datetime.date(
    previous_day.year, previous_day.month, 1
) %}

-- Queries
with
    sms as (select * from {{ ref("mart_gerenciamento_custo_gcp") }} where orgao = "SMS")

select *
from sms
{% if is_incremental() %}
    where invoice_competencia_particao = '{{ partition_to_replace }}'
{% endif %}
