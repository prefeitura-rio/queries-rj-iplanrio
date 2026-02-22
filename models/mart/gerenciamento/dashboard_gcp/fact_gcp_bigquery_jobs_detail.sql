{{
    config(
        materialized='view',
        schema='dashboard_gcp',
        alias='fact_gcp_bigquery_jobs_detail',
        tags=['dashboard', 'gcp', 'bigquery', 'detail'],
    )
}}

-- View detalhada de jobs BigQuery para drill-down no dashboard
-- Une raw_gcp_bigquery_jobs_v2 com raw_gcp_bigquery_cost_allocated_v1 para ter job details + cost
-- IMPORTANTE: Sempre usar com filtros de data no Looker para evitar scan completo

WITH jobs_with_cost AS (
    SELECT
        j.project_id,
        j.job_id,
        j.orgao,
        j.ambiente,
        j.projeto_base,
        j.principal_email,
        j.principal_type,
        j.is_service_account,
        j.user_email,
        j.job_type,
        j.statement_type,
        j.destination_project_id,
        j.destination_dataset_id,
        j.destination_table_id,
        j.creation_time,
        j.end_time,
        j.job_date_utc,
        j.job_month_utc,
        c.invoice_month_date,
        c.total_bytes_billed,
        c.total_bytes_project_month,
        c.bigquery_cost_gross,
        c.bigquery_credits,
        c.bigquery_cost_net,
        c.allocated_cost_job,
        c.job_cost_proportion
    FROM {{ ref('raw_gcp_bigquery_jobs_v2') }} j
    INNER JOIN {{ ref('raw_gcp_bigquery_cost_allocated_v1') }} c
        ON j.project_id = c.project_id
        AND j.job_id = c.job_id
    WHERE c.allocated_cost_job > 0
)

SELECT
    -- Identificação
    project_id,
    job_id,
    orgao,
    ambiente,
    projeto_base,

    -- Principal (usuário/service account)
    principal_email,
    principal_type,
    is_service_account,
    user_email,

    -- Job info
    job_type,
    statement_type,
    CASE
        WHEN destination_table_id IS NOT NULL
        THEN CONCAT(
            COALESCE(destination_project_id, project_id),
            '.',
            destination_dataset_id,
            '.',
            destination_table_id
        )
        ELSE NULL
    END AS destination_table_full_name,

    -- Timestamps
    creation_time,
    end_time,
    job_date_utc,
    job_month_utc,
    invoice_month_date,

    -- Bytes e uso
    total_bytes_billed,
    total_bytes_project_month,
    ROUND(total_bytes_billed / POWER(1024, 4), 4) AS tib_billed,

    -- Custos
    bigquery_cost_gross,
    bigquery_credits,
    bigquery_cost_net,
    allocated_cost_job,
    job_cost_proportion,

    -- Métricas derivadas
    CASE
        WHEN total_bytes_billed > 0
        THEN ROUND(allocated_cost_job / (total_bytes_billed / POWER(1024, 4)), 4)
        ELSE 0
    END AS cost_per_tib

FROM jobs_with_cost
