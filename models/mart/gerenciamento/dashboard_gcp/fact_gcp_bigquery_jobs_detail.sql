{{
    config(
        materialized='view',
        schema='dashboard_gcp',
        alias='fact_gcp_bigquery_jobs_detail',
        tags=['dashboard', 'gcp', 'bigquery', 'detail'],
    )
}}

-- View detalhada de jobs BigQuery para drill-down técnico
-- Granularidade: job individual (milhões de linhas)
-- ⚠️ IMPORTANTE: Sempre usar com filtro de invoice_month_date no Looker Studio

SELECT
    project_id,
    job_id,
    principal_email,
    user_email,
    principal_type,
    is_service_account,

    -- Classificação
    orgao,
    ambiente,
    projeto_base,
    job_type,
    statement_type,

    -- Temporal
    creation_time,
    end_time,
    job_date_utc,
    job_month_utc,
    invoice_month_date,

    -- Recursos e Custo
    total_bytes_billed,
    ROUND(total_bytes_billed / POWER(1024, 4), 4) AS tib_billed,  -- TiB formatado
    total_bytes_project_month,
    allocated_cost_job,

    -- Métricas derivadas
    SAFE_DIVIDE(total_bytes_billed, total_bytes_project_month) AS job_cost_proportion,

    -- Custo por TiB
    CASE
        WHEN total_bytes_billed > 0
        THEN ROUND(allocated_cost_job / (total_bytes_billed / POWER(1024, 4)), 4)
        ELSE 0
    END AS cost_per_tib

FROM {{ ref('raw_gcp_bigquery_cost_allocated_v1') }}
WHERE allocated_cost_job > 0  -- Apenas jobs com custo alocado
