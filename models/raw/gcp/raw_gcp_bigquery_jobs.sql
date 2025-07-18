{{
    config(
        schema="brutos_gcp",
        alias="gcp_bigquery_jobs",
        materialized="incremental",
        full_refresh = false,
        unique_key=["project_id", "job_id"],
        partition_by={
            "field": "data_faturamento",
            "data_type": "date",
            "granularity": "month"
        },
        tags=["daily"]
    )
}}

-- Definição do período incremental
{% if is_incremental() and execute %}
  {% set start_date_query %}
    SELECT COALESCE(MAX(data_faturamento), DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)) AS start_date
    FROM {{ this }}
  {% endset %}
  {% if execute %}
    {% set results = run_query(start_date_query) %}
    {% set start_date = results.columns[0].values()[0] %}
  {% else %}
    {% set start_date = '2023-01-01' %}  -- valor default para compilação
  {% endif %}
{% else %}
  {% set start_date = (modules.datetime.datetime.utcnow() - modules.datetime.timedelta(days=180)).strftime('%Y-%m-%d') %}
{% endif %}


{% set projetos = [
    'rj-cetrio',
    'rj-cetrio-dev',
    'rj-chatbot',""
    'rj-chatbot-dev',
    'rj-civitas',
    'rj-civitas-dev',
    'rj-cmp',
    'rj-comunicacao',
    'rj-comunicacao-dev',
    'rj-cor',
    'rj-cor-dev',
    'rj-crm-registry',
    'rj-crm-registry-dev',
    'rj-cvl',
    'rj-cvl-dev',
    'rj-datalab-sandbox',
    'rj-escritorio',
    'rj-escritorio-dev',
    'rj-iplanrio',
    'rj-iplanrio-dev',
    'rj-iplanrio-ia-dev',
    'rj-ipp',
    'rj-ipp-dev',
    'rj-mapa-realizacoes',
    'rj-mapa-realizacoes-dev',
    'rj-multirio',
    'rj-multirio-dev',
    'rj-pgm',
    'rj-precipitacao',
    'rj-rec-rio',
    'rj-rec-rio-dev',
    'rj-rioaguas',
    'rj-rioaguas-dev',
    'rj-riosaude',
    'rj-seconserva',
    'rj-seconserva-dev',
    'rj-segovi',
    'rj-segovi-dev',
    'rj-seop',
    'rj-seop-dev',
    'rj-setur',
    'rj-setur-dev',
    'rj-siurb',
    'rj-smac',
    'rj-smac-dev',
    'rj-smas',
    'rj-smas-dev',
    'rj-smas-dev-432320',
    'rj-smdue',
    'rj-sme',
    'rj-sme-dev',
    'rj-smfp',
    'rj-smfp-dev',
    'rj-smfp-egp',
    'rj-smi',
    'rj-smi-dev',
    'rj-sms',
    'rj-sms-dev',
    'rj-sms-sandbox',
    'rj-smtr',
    'rj-smtr-dev',
    'rj-smtr-staging',
    'rj-vision-ai',
    'rj-vision-ai-dev'
] %}

{% set all_queries = [] %}
{% for projeto in projetos %}
    {% set q %}
        select
            '{{ projeto }}' as origem_projeto,
            project_id,
            job_id,
            user_email,
            job_type,
            query,
            state,
            destination_table.project_id as destination_project_id,
            destination_table.dataset_id as destination_dataset_id,
            destination_table.table_id as destination_table_id,
            error_result,
            creation_time,
            end_time,
            statement_type,
            total_bytes_processed,
            total_bytes_billed,
            extract(date from end_time at time zone 'PST8PDT') as data_faturamento
        from `{{ projeto }}`.`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        where DATE(creation_time) > DATE('{{ start_date }}')
    {% endset %}
    {% do all_queries.append(q) %}
{% endfor %}

with all_usage as (
    {{ all_queries | join('\nunion all\n') }}
),

deduped_usage as (
    select * except(rn) from (
        select *,
            row_number() over (partition by project_id, job_id order by creation_time desc) as rn
        from all_usage
    ) where rn = 1
)

select
    origem_projeto,
    project_id,
    job_id,
    user_email,
    job_type,
    query,
    state,
    destination_project_id,
    destination_dataset_id,
    destination_table_id,
    error_result,
    creation_time,
    end_time,
    statement_type,
    total_bytes_processed,
    total_bytes_billed,
    data_faturamento
from deduped_usage
