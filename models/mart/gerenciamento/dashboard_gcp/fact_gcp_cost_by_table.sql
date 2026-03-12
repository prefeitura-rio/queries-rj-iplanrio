{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "invoice_month_date",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by=["job_project_id", "dataset_id"],
        alias='fact_gcp_cost_by_table',
        tags=['dashboard', 'gcp', 'cost', 'table', 'dataset'],
    )
}}

{% set lookback_months = 3 %}
{% set lookback_days = lookback_months * 30 + 30 %}  -- 3 meses + buffer de 30 dias = 120 dias
-- Buffer adicional garante que todos os jobs dos últimos 3 meses sejam capturados,
-- considerando variação de dias por mês e possíveis delays no INFORMATION_SCHEMA

-- Modelo de custo BigQuery por dataset e tabela
-- Granularidade: invoice_month_date + project_id + dataset_id + table_id
-- Permite identificar quais tabelas/datasets geram mais custo

WITH cost_allocated AS (
    -- Custos alocados por job (base de custo)
    SELECT
        invoice_month_date,
        project_id,
        job_id,
        orgao,
        ambiente,
        projeto_base,
        principal_email,
        principal_type,
        is_service_account,
        allocated_cost_job,
        total_bytes_billed,
        creation_time
    FROM {{ ref('raw_gcp_bigquery_cost_allocated_v1') }}
    WHERE allocated_cost_job > 0  -- Apenas jobs com custo

    {% if is_incremental() %}
        AND invoice_month_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL {{ lookback_months }} MONTH), MONTH)
    {% endif %}
),

-- CTE para buscar referenced_tables de cada job
-- Nota: Este será um UNION ALL de todos os projetos (similar ao raw_gcp_bigquery_jobs_v2)
{% set projetos = [
    "rj-cetrio", "rj-cetrio-dev", "rj-chatbot", "rj-chatbot-dev", "rj-civitas", "rj-civitas-dev",
    "rj-cmp", "rj-comunicacao", "rj-comunicacao-dev", "rj-cor", "rj-cor-dev",
    "rj-crm-registry", "rj-crm-registry-dev", "rj-cvl", "rj-cvl-dev", "rj-datalab-sandbox",
    "rj-ia-desenvolvimento", "rj-escritorio", "rj-escritorio-dev", "rj-iplanrio", "rj-iplanrio-dev",
    "rj-iplanrio-ia-dev", "rj-ipp", "rj-ipp-dev", "rj-mapa-realizacoes", "rj-mapa-realizacoes-dev",
    "rj-multirio", "rj-multirio-dev", "rj-pgm", "rj-precipitacao", "rj-rec-rio", "rj-rec-rio-dev",
    "rj-rioaguas", "rj-rioaguas-dev", "rj-riosaude", "rj-seconserva", "rj-seconserva-dev",
    "rj-segovi", "rj-segovi-dev", "rj-seop", "rj-seop-dev", "rj-setur", "rj-setur-dev",
    "rj-siurb", "rj-smac", "rj-smac-dev", "rj-smas", "rj-smas-dev", "rj-smdue",
    "rj-sme", "rj-sme-dev", "rj-smfp", "rj-smfp-dev", "rj-smfp-egp", "rj-smi", "rj-smi-dev",
    "rj-sms", "rj-sms-dev", "rj-sms-sandbox", "rj-smtr", "rj-smtr-dev", "rj-smtr-staging"
] %}

{% set region = 'region-us' %}

{% set all_ref_tables = [] %}
{% for projeto in projetos %}
    {% set q %}
        SELECT
            jobs.project_id,
            jobs.job_id,
            ref_table.project_id AS ref_project_id,
            ref_table.dataset_id AS ref_dataset_id,
            ref_table.table_id AS ref_table_id
        FROM `{{ projeto }}`.`{{ region }}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT AS jobs,
            UNNEST(referenced_tables) AS ref_table
        WHERE DATE(jobs.creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ lookback_days }} DAY)
            AND jobs.state = 'DONE'
            AND jobs.total_bytes_billed > 0
    {% endset %}
    {% do all_ref_tables.append(q) %}
{% endfor %}

jobs_referenced_tables AS (
    {{ all_ref_tables | join("\n    UNION ALL\n    ") }}
),

-- Deduplica tabelas referenciadas (um job pode referenciar a mesma tabela múltiplas vezes)
jobs_tables_deduped AS (
    SELECT DISTINCT
        project_id,
        job_id,
        ref_project_id,
        ref_dataset_id,
        ref_table_id
    FROM jobs_referenced_tables
    WHERE ref_table_id IS NOT NULL
        AND ref_dataset_id IS NOT NULL
),

-- Join custos com tabelas referenciadas
cost_by_table_raw AS (
    SELECT
        c.invoice_month_date,
        c.project_id AS job_project_id,
        c.orgao,
        c.ambiente,
        c.projeto_base,
        c.job_id,
        c.principal_email,
        c.principal_type,
        c.is_service_account,
        c.allocated_cost_job,
        c.total_bytes_billed,
        c.creation_time,

        -- Tabelas referenciadas
        t.ref_project_id AS table_project_id,
        t.ref_dataset_id AS dataset_id,
        t.ref_table_id AS table_id,

        -- Contagem de tabelas referenciadas por job (para divisão proporcional de custo)
        COUNT(*) OVER (PARTITION BY c.job_id) AS tables_count_in_job

    FROM cost_allocated c
    INNER JOIN jobs_tables_deduped t
        ON c.project_id = t.project_id
        AND c.job_id = t.job_id
),

-- Aloca custo proporcionalmente entre as tabelas referenciadas
cost_allocated_per_table AS (
    SELECT
        invoice_month_date,
        job_project_id,
        orgao,
        ambiente,
        projeto_base,
        table_project_id,
        dataset_id,
        table_id,
        job_id,
        principal_email,
        principal_type,
        is_service_account,
        creation_time,

        -- Custo dividido entre tabelas (se job lê 3 tabelas, cada uma recebe 1/3)
        SAFE_DIVIDE(allocated_cost_job, tables_count_in_job) AS allocated_cost_table,

        -- Bytes também divididos proporcionalmente
        SAFE_DIVIDE(total_bytes_billed, tables_count_in_job) AS allocated_bytes_table,

        tables_count_in_job

    FROM cost_by_table_raw
),

-- Agregação final por mês + projeto + dataset + tabela
aggregated_by_table AS (
    SELECT
        invoice_month_date,
        job_project_id,
        table_project_id,
        dataset_id,
        table_id,
        orgao,
        ambiente,
        projeto_base,

        -- Métricas agregadas
        SUM(allocated_cost_table) AS total_cost,
        COUNT(DISTINCT job_id) AS jobs_count,
        COUNT(DISTINCT principal_email) AS unique_users_count,
        COUNT(DISTINCT CASE WHEN principal_type = 'human' THEN principal_email END) AS unique_human_users_count,
        COUNT(DISTINCT CASE WHEN is_service_account THEN principal_email END) AS unique_service_accounts_count,
        SUM(allocated_bytes_table) AS total_bytes_read,

        -- Primeira e última leitura
        MIN(creation_time) AS first_access_time,
        MAX(creation_time) AS last_access_time,

        -- Dias ativos
        COUNT(DISTINCT DATE(creation_time)) AS active_days_in_month

    FROM cost_allocated_per_table
    GROUP BY
        invoice_month_date,
        job_project_id,
        table_project_id,
        dataset_id,
        table_id,
        orgao,
        ambiente,
        projeto_base
),

-- Adiciona rankings e percentuais
with_rankings AS (
    SELECT
        *,

        -- Ranking de custo (geral)
        ROW_NUMBER() OVER (
            PARTITION BY invoice_month_date
            ORDER BY total_cost DESC
        ) AS cost_rank_global,

        -- Ranking de custo por órgão
        ROW_NUMBER() OVER (
            PARTITION BY invoice_month_date, orgao
            ORDER BY total_cost DESC
        ) AS cost_rank_by_orgao,

        -- Ranking de custo por projeto
        ROW_NUMBER() OVER (
            PARTITION BY invoice_month_date, job_project_id
            ORDER BY total_cost DESC
        ) AS cost_rank_by_project,

        -- Percentual do custo total do órgão
        SAFE_DIVIDE(
            total_cost,
            SUM(total_cost) OVER (PARTITION BY invoice_month_date, orgao)
        ) * 100 AS pct_of_orgao_cost,

        -- Percentual do custo total do projeto
        SAFE_DIVIDE(
            total_cost,
            SUM(total_cost) OVER (PARTITION BY invoice_month_date, job_project_id)
        ) * 100 AS pct_of_project_cost,

        -- TiB lidos
        ROUND(total_bytes_read / POWER(1024, 4), 4) AS total_tib_read,

        -- Custo por TiB
        SAFE_DIVIDE(total_cost, total_bytes_read / POWER(1024, 4)) AS cost_per_tib

    FROM aggregated_by_table
)

SELECT
    invoice_month_date,
    job_project_id,
    table_project_id,
    dataset_id,
    table_id,
    orgao,
    ambiente,
    projeto_base,

    -- Custos
    total_cost,
    cost_per_tib,

    -- Uso
    jobs_count,
    unique_users_count,
    unique_human_users_count,
    unique_service_accounts_count,
    total_bytes_read,
    total_tib_read,

    -- Temporal
    first_access_time,
    last_access_time,
    active_days_in_month,

    -- Rankings
    cost_rank_global,
    cost_rank_by_orgao,
    cost_rank_by_project,

    -- Percentuais
    ROUND(pct_of_orgao_cost, 2) AS pct_of_orgao_cost,
    ROUND(pct_of_project_cost, 2) AS pct_of_project_cost,

    -- Flags úteis
    cost_rank_global <= 100 AS is_top_100_expensive_table,
    cost_rank_by_project <= 10 AS is_top_10_in_project,
    active_days_in_month >= 20 AS is_frequently_accessed

FROM with_rankings

{% if is_incremental() %}
WHERE invoice_month_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL {{ lookback_months }} MONTH), MONTH)
{% endif %}
