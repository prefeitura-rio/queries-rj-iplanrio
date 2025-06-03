-- Custos do BigQuery abertos por execução
-- Ref: https://cloud.google.com/bigquery/docs/information-schema-jobs#compare_on-demand_job_usage_to_billing_data

{{
    config(
        schema="gerenciamento_custos",
        alias="gcp_billing_bigquery_detalhado",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data_faturamento",
            "data_type": "date",
            "granularity": "month",
        },
        tags=["daily"],
    )
}}

{% set first_day_of_month = "date_trunc(current_date('America/Sao_Paulo'), month)" %}

-- Data inicial para coleta dos dados de faturamento
{% set start_date = "2024-01-01" %}
-- Data atual para limite superior da coleta
{% set end_date = modules.datetime.datetime.now().strftime("%Y-%m-%d") %}

-- SOURCES

{% set projetos = [
    'rj-cetrio',
    'rj-cetrio-dev',
    'rj-chatbot',
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
            total_bytes_billed
        from `{{ projeto }}`.`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        where DATE(creation_time) >= DATE('{{ start_date }}')
          and DATE(creation_time) <= DATE('{{ end_date }}')
    {% endset %}
    {% do all_queries.append(q) %}
{% endfor %}

with all_usage as (
    {{ all_queries | join('\nunion all\n') }}
),

all_usage_with_multiplier as (
    select
        origem_projeto,
        project_id as projeto_id,
        job_id as id_job,
        user_email as email_usuario,
        job_type as tipo_job,
        query as consulta_sql,
        state as estado_job,
        destination_project_id as projeto_destino_id,
        destination_dataset_id as dataset_destino_id,
        destination_table_id as tabela_destino_id,
        error_result as resultado_erro,
        creation_time as horario_criacao,
        extract(date from end_time at time zone 'PST8PDT') as data_faturamento,
        total_bytes_processed / 1024 / 1024 / 1024 / 1024 as tib_processado,
        total_bytes_billed / 1024 / 1024 / 1024 / 1024 as tib_faturado,
        case
            statement_type
                when 'SCRIPT' then 0
                when 'CREATE_MODEL' then 50 * 6.25
                else 6.25
        end as multiplicador
    from all_usage
),

cost_added as (
    select
        *,
        tib_faturado * multiplicador as custo_estimado_usd,
        tib_faturado as uso_estimado_tib,
        case
            when tib_faturado is not null then true
            else false
        end as job_faturavel  -- indica se foi cobrado (custo > 0)
    from all_usage_with_multiplier
),

final as (
    select
        regexp_extract(projeto_id, r'^rj-([a-z0-9]+)') as orgao,
        origem_projeto,
        projeto_id,
        id_job,
        horario_criacao as datahora_criacao,
        email_usuario,
        tipo_job,
        consulta_sql,
        estado_job,
        struct(
            projeto_destino_id as projeto_id,
            regexp_replace(
                dataset_destino_id,
                r'(dev_fantasma__|diego__|miloskimatheus__|pedro__|thiago__|vit__)',
                ''
            ) as dataset_id, -- ver se inclui o pessoal da iplan dps
            tabela_destino_id as tabela_id
        ) as destino,
        tib_processado as tib_processado_quantidade,
        resultado_erro,
        data_faturamento,
        uso_estimado_tib as uso_estimado_tib_quantidade,
        custo_estimado_usd as custo_estimado_valor,
        job_faturavel
    from cost_added
)

select *
from final
{% if is_incremental() %} where data_faturamento >= {{ first_day_of_month }} {% endif %}
