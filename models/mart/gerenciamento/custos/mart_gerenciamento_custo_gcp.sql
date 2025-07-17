{{
    config(
        schema="gerenciamento_custos",
        alias="custo_gcp",
        materialized="incremental",
        unique_key=["billing_account_id", "service_id", "sku_id", "usage_start_time", "project_id"],
        incremental_strategy="merge",
        partition_by={
            "field": "usage_end_time",
            "data_type": "timestamp",
            "granularity": "month"
        },
        cluster_by=["orgao", "project_id", "service_id"],
        tags=["custo_gcp", "incremental"]
    )
}}

{% set orgao_mapping = {
    'crm-registry': 'IPLANRIO',
    'rj-comunicacao-dev': 'IPLANRIO',
    'rj-superapp': 'IPLANRIO',
    'rj-vision-ai': 'IPLANRIO',
    'dados-rio-billing': 'IPLANRIO',
    'rj-chatbot': 'IPLANRIO',
    'rj-chatbot-dev': 'IPLANRIO'
} %}

with base_data as (
    select
        billing_account_id,
        service.id as service_id,
        service.description as service_description,
        sku.id as sku_id,
        sku.description as sku_description,
        project.id as project_id,
        project.number as project_number,
        project.name as project_name,
        usage_start_time,
        usage_end_time,
        cost,
        cost_at_effective_price_default,
        cost_at_list_consumption_model,
        coalesce(
            (select sum(c.amount) from unnest(credits) as c),
            0
        ) as desconto_em_reais,
        location.location,
        location.country,
        location.region,
        location.zone,
        resource.name as resource_name,
        resource.global_name as resource_global_name,
        export_time,
        labels,
        tags,
        price

    from {{ ref('raw_gcp_billing') }}
    {% if is_incremental() %}
        where usage_end_time > (
            select coalesce(max(usage_end_time), timestamp('1970-01-01'))
            from {{ this }}
        )
    {% endif %}
),

transformed_data as (
    select
        *,
        cost + desconto_em_reais as custo_liquido,

        -- Agrupamento otimizado de resource name
        case
            when resource_name like '%instances/%' and resource_name like '%-pool%'
                then regexp_extract(resource_name, r'instances/([^/-]+(?:-[^/-]+)*-pool)')
            when resource_name like '%gke-datario%'
                then 'GKE-DATARIO'
            when resource_name like '%gke-datalake%'
                then 'GKE-DATALAKE'
            when resource_name like '%gke-application%'
                then 'GKE-APPLICATION'
            when resource_name like '%job_%'
                then 'JOB'
            when resource_name like '%/secrets/%'
                then 'SECRET MANAGER'
            when resource_name like '%istio%ingress%istio%'
                then 'ISTIO_INGRESS'
            else resource_name
        end as agrupamento_resource_name,


        case
            when project_id is null then 'SND'
            {% for project, orgao in orgao_mapping.items() %}
            when project_id = '{{ project }}' then '{{ orgao }}'
            {% endfor %}
            when project_id like '%hackathon%' then 'IPLANRIO'
            when project_id like '%datario%' then 'IPLANRIO'
            when project_id like 'rj-rec%' then 'RECRIO'
            when project_id like 'rj-%' then upper(regexp_extract(project_id, r'^rj-([^-\s]+)'))
            else upper(project_id)
        end as orgao

    from base_data
)

select
    billing_account_id,
    orgao,
    project_id,
    project_name,
    project_number,
    service_id,
    service_description,
    sku_id,
    sku_description,
    usage_start_time,
    usage_end_time,
    cost,
    desconto_em_reais,
    custo_liquido,
    cost_at_effective_price_default,
    cost_at_list_consumption_model,
    location,
    country,
    region,
    zone,
    resource_name,
    agrupamento_resource_name,
    resource_global_name,
    export_time,
    labels,
    tags,
    price
from transformed_data
