{{
    config(
        schema="gerenciamento_custos",
        alias="gcp_billing",
        materialized="table",
        partition_by={
            "field": "data_faturamento",
            "data_type": "date",
            "granularity": "month"
        }
    )
}}

with 
source_data as (
    select * from {{ ref('raw_gcp_billing') }}
),

base_data as (
    select
        *,
        case
            when regexp_contains(resource.name, r'instances/')
            then regexp_extract(resource.name, r'instances/([^/-]+(?:-[^/-]+)*-pool)')
            when regexp_contains(resource.name, r'gke-datario')
            then 'GKE-DATARIO'
            when regexp_contains(resource.name, r'gke-datalake')
            then 'GKE-DATALAKE'
            when regexp_contains(resource.name, r'gke-application')
            then 'GKE-APPLICATION'
            when regexp_contains(resource.name, r'(job_[^/]+)')
            then 'JOB'
            when regexp_contains(resource.name, r'/secrets/')
            then 'SECRET MANAGER'
            when regexp_contains(resource.name, r'istio.*ingress.*istio')
            then 'ISTIO_INGRESS'
            else resource.name
        end as agrupamento_resource_name,
        case
            when project.id is null
            then 'SND'
            when
                project.id in (
                    'crm-registry',
                    'rj-comunicacao-dev',
                    'rj-superapp',
                    'rj-vision-ai',
                    'dados-rio-billing',
                    'rj-chatbot',
                    'rj-chatbot-dev'
                )
            then 'IPLANRIO'
            when regexp_contains(project.id, r'hackathon')
            then 'IPLANRIO'
            when regexp_contains(project.id, r'datario')
            then 'IPLANRIO'
            when regexp_contains(project.id, r'^rj-rec')
            then 'RECRIO'
            when regexp_contains(project.id, r'^rj-')
            then upper(regexp_extract(project.id, r'^rj-([^-\s]+)'))
            else upper(project.id)
        end as orgao
    from source_data
),

credits_summary as (
    select
        *,
        coalesce(sum(c.amount) over (), 0) as desconto_em_reais
    from base_data,
    unnest(credits) as c
)

select * from credits_summary
