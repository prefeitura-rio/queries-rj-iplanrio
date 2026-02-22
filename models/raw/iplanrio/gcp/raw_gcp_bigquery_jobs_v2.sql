{{
    config(
        enabled=true,
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        schema='brutos_gcp',
        alias='gcp_bigquery_jobs_v2',
        unique_key=['project_id', 'job_id'],
        partition_by={
            "field": "job_month_utc",
            "data_type": "date",
            "granularity": "month",
        },
        tags=['gcp_bigquery_jobs', 'incremental', 'v2'],
    )
}}

-- Jobs do BigQuery V2 com calendário UTC (sem PST8PDT)
-- Adiciona classificação de principal e join com dimensão de projetos

{% set lookback_days = 7 %}
{% set region = 'region-us' %}

{% set projetos = [
    "rj-cetrio",
    "rj-cetrio-dev",
    "rj-chatbot",
    "rj-chatbot-dev",
    "rj-civitas",
    "rj-civitas-dev",
    "rj-cmp",
    "rj-comunicacao",
    "rj-comunicacao-dev",
    "rj-cor",
    "rj-cor-dev",
    "rj-crm-registry",
    "rj-crm-registry-dev",
    "rj-cvl",
    "rj-cvl-dev",
    "rj-datalab-sandbox",
    "rj-ia-desenvolvimento",
    "rj-escritorio",
    "rj-escritorio-dev",
    "rj-iplanrio",
    "rj-iplanrio-dev",
    "rj-iplanrio-ia-dev",
    "rj-ipp",
    "rj-ipp-dev",
    "rj-mapa-realizacoes",
    "rj-mapa-realizacoes-dev",
    "rj-multirio",
    "rj-multirio-dev",
    "rj-pgm",
    "rj-precipitacao",
    "rj-rec-rio",
    "rj-rec-rio-dev",
    "rj-rioaguas",
    "rj-rioaguas-dev",
    "rj-riosaude",
    "rj-seconserva",
    "rj-seconserva-dev",
    "rj-segovi",
    "rj-segovi-dev",
    "rj-seop",
    "rj-seop-dev",
    "rj-setur",
    "rj-setur-dev",
    "rj-siurb",
    "rj-smac",
    "rj-smac-dev",
    "rj-smas",
    "rj-smas-dev",
    "rj-smdue",
    "rj-sme",
    "rj-sme-dev",
    "rj-smfp",
    "rj-smfp-dev",
    "rj-smfp-egp",
    "rj-smi",
    "rj-smi-dev",
    "rj-sms",
    "rj-sms-dev",
    "rj-sms-sandbox",
    "rj-smtr",
    "rj-smtr-dev",
    "rj-smtr-staging",
] %}

-- Filtro de data para incremental
{% if is_incremental() %}
    {% set date_filter = "DATE(creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL " ~ lookback_days ~ " DAY)" %}
{% else %}
    {% set date_filter = "DATE(creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)" %}
{% endif %}

-- Gerar queries para todos os projetos
{% set all_queries = [] %}
{% for projeto in projetos %}
    {% set q %}
        SELECT
            '{{ projeto }}' AS origem_projeto,
            project_id,
            job_id,
            user_email,
            job_type,
            query,
            state,
            destination_table.project_id AS destination_project_id,
            destination_table.dataset_id AS destination_dataset_id,
            destination_table.table_id AS destination_table_id,
            error_result,
            creation_time,
            end_time,
            statement_type,
            total_bytes_processed,
            total_bytes_billed
        FROM `{{ projeto }}`.`{{ region }}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        WHERE {{ date_filter }}
    {% endset %}
    {% do all_queries.append(q) %}
{% endfor %}

WITH
    all_jobs AS (
        {{ all_queries | join("\n        UNION ALL\n        ") }}
    ),

    -- Deduplica por project_id + job_id
    deduped_jobs AS (
        SELECT * EXCEPT (rn)
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY project_id, job_id ORDER BY creation_time DESC
                ) AS rn
            FROM all_jobs
        )
        WHERE rn = 1
    ),

    -- Adiciona campos UTC e classificação de principal
    jobs_with_fields AS (
        SELECT
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

            -- Calendário UTC (sem PST8PDT)
            DATE(creation_time) AS job_date_utc,
            DATE_TRUNC(DATE(creation_time), MONTH) AS job_month_utc,

            -- Principal classification
            LOWER(user_email) AS principal_email,

            REGEXP_CONTAINS(user_email, r'gserviceaccount\.com$') AS is_service_account,

            CASE
                WHEN user_email IS NULL THEN 'unknown'
                WHEN REGEXP_CONTAINS(user_email, r'@iam\.gserviceaccount\.com$') THEN 'service_account'
                WHEN REGEXP_CONTAINS(user_email, r'gserviceaccount\.com$') THEN 'service_account'
                WHEN REGEXP_CONTAINS(user_email, r'@.*\.iam\.gserviceaccount\.com$') THEN 'service_account'
                WHEN REGEXP_CONTAINS(user_email, r'^[0-9]+-compute@developer\.gserviceaccount\.com$') THEN 'compute_engine'
                WHEN REGEXP_CONTAINS(user_email, r'@cloudservices\.gserviceaccount\.com$') THEN 'google_apis'
                WHEN REGEXP_CONTAINS(user_email, r'\.google\.com$') THEN 'google_apis'
                WHEN REGEXP_CONTAINS(user_email, r'@.*\.gserviceaccount\.com$') THEN 'service_account'
                ELSE 'human'
            END AS principal_type

        FROM deduped_jobs
    )

-- Query final: join com dimensão de projetos
SELECT
    jobs.origem_projeto,
    jobs.project_id,
    jobs.job_id,
    jobs.user_email,
    jobs.principal_email,
    jobs.is_service_account,
    jobs.principal_type,
    jobs.job_type,
    jobs.query,
    jobs.state,
    jobs.destination_project_id,
    jobs.destination_dataset_id,
    jobs.destination_table_id,
    jobs.error_result,
    jobs.creation_time,
    jobs.end_time,
    jobs.statement_type,
    jobs.total_bytes_processed,
    jobs.total_bytes_billed,
    jobs.job_date_utc,
    jobs.job_month_utc,

    -- Campos da dimensão
    dim.orgao,
    dim.ambiente,
    dim.projeto_base

FROM jobs_with_fields jobs
LEFT JOIN {{ ref('raw_dim_gcp_project') }} dim
    ON jobs.project_id = dim.project_id

{% if is_incremental() %}
WHERE jobs.job_month_utc >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL {{ lookback_days }} DAY), MONTH)
{% endif %}
