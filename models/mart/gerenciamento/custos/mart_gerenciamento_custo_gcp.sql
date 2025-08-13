{{
    config(
        schema="gerenciamento_custos",
        alias="custo_gcp",
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

{% set iplan = [
    "dados-rio-billing",
    "datario",
    "datario-dev",
    "hackathon-fgv-03-2024",
    "rj-caio",
    "rj-chatbot",
    "rj-chatbot-dev",
    "rj-crm-registry",
    "rj-crm-registry-dev",
    "rj-comunicacao",
    "rj-comunicacao-dev",
    "rj-datalab-sandbox",
    "rj-escritorio",
    "rj-escritorio-dev",
    "rj-ia-desenvolvimento",
    "rj-mapa-realizacoes",
    "rj-mapa-realizacoes-dev",
    "rj-precipitacao",
    "rj-superapp",
    "rj-superapp-staging",
    "rj-vision-ai",
] %}


-- Queries
with
    source as (select * from {{ ref("raw_gcp_billing") }}),

    fixed_credits as (
        select
            * except (credits),
            coalesce((select sum(c.amount) from unnest(credits) as c), 0) as credits
        from source
    ),

    transformed_data as (
        select
            *,

            cost + credits as cost_with_credits,

            case
                when project.id is null
                then 'NÃO DEFINIDO'
                when project.id in ('{{ iplan | join("', '") }}')
                then 'IPLANRIO'
                when project.id like 'rj-rec%'
                then 'RECRIO'
                when project.id like 'rj-%'
                then upper(regexp_extract(project.id, r'^rj-([^-\s]+)'))
                else upper(project.id)
            end as orgao

        from fixed_credits
    )

select *
from transformed_data
{% if is_incremental() %}
    where invoice_competencia_particao = '{{ partition_to_replace }}'
{% endif %}
