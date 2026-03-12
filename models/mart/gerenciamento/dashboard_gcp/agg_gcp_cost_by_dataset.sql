{{
    config(
        materialized='view',
        alias='agg_gcp_cost_by_dataset',
        tags=['dashboard', 'gcp', 'aggregation', 'dataset'],
    )
}}

-- View de agregação de custos por dataset (sem granularidade de tabela)
-- Propósito: Análise mais macro de custos por dataset
-- Granularidade: invoice_month_date + job_project_id + dataset_id

SELECT
    invoice_month_date,
    job_project_id,
    table_project_id,
    dataset_id,
    orgao,
    ambiente,
    projeto_base,

    -- Agregações de custo e uso
    SUM(total_cost) AS total_cost,
    AVG(cost_per_tib) AS avg_cost_per_tib,
    SUM(jobs_count) AS total_jobs,
    SUM(unique_users_count) AS total_unique_users,
    SUM(unique_human_users_count) AS total_unique_human_users,
    SUM(unique_service_accounts_count) AS total_unique_service_accounts,
    SUM(total_bytes_read) AS total_bytes_read,
    SUM(total_tib_read) AS total_tib_read,

    -- Contagem de tabelas no dataset
    COUNT(DISTINCT table_id) AS tables_count,

    -- Temporal
    MIN(first_access_time) AS first_access_time,
    MAX(last_access_time) AS last_access_time,
    MAX(active_days_in_month) AS max_active_days_in_month,

    -- Estatísticas de distribuição de custo entre tabelas
    MIN(total_cost) AS min_table_cost,
    MAX(total_cost) AS max_table_cost,
    AVG(total_cost) AS avg_table_cost,
    STDDEV(total_cost) AS stddev_table_cost,

    -- Contagem de hot tables (top 10 do projeto)
    SUM(CASE WHEN is_top_10_in_project THEN 1 ELSE 0 END) AS hot_tables_count,

    -- Contagem de frequently accessed tables
    SUM(CASE WHEN is_frequently_accessed THEN 1 ELSE 0 END) AS frequently_accessed_tables_count

FROM {{ ref('fact_gcp_cost_by_table') }}
GROUP BY
    invoice_month_date,
    job_project_id,
    table_project_id,
    dataset_id,
    orgao,
    ambiente,
    projeto_base
ORDER BY invoice_month_date DESC, total_cost DESC
