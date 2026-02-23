{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        schema='dashboard_gcp',
        alias='fact_gcp_cost_monthly',
        partition_by={
            "field": "invoice_month_date",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by=["orgao", "service_description"],
        tags=['dashboard', 'gcp', 'cost', 'monthly'],
    )
}}

{% set lookback_months = 3 %}

WITH billing_monthly AS (
        SELECT
            project.id AS project_id,
            DATE(CONCAT(LEFT(invoice.month, 4), '-', RIGHT(invoice.month, 2), '-01')) AS invoice_month_date,
            service.description AS service_description,
            SUM(cost) AS cost_gross,
            SUM(COALESCE((SELECT SUM(c.amount) FROM UNNEST(credits) AS c), 0)) AS credits,
            SUM(cost) + SUM(COALESCE((SELECT SUM(c.amount) FROM UNNEST(credits) AS c), 0)) AS cost_net,
            SUM(usage.amount) AS usage_amount

        FROM {{ ref('raw_gcp_billing') }}
        WHERE cost_type = 'regular'
            AND project.id IS NOT NULL

        {% if is_incremental() %}
            AND invoice_competencia_particao >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL {{ lookback_months }} MONTH), MONTH)
        {% endif %}

        GROUP BY project_id, invoice_month_date, service_description
    ),

bigquery_stats AS (
        SELECT
            project_id,
            invoice_month_date,

            COUNT(DISTINCT CASE WHEN principal_type = 'user' THEN principal_email END) AS active_users_count,
            COUNT(DISTINCT CASE WHEN is_service_account THEN principal_email END) AS active_service_accounts_count,
            COUNT(DISTINCT job_id) AS jobs_count,
            SUM(total_bytes_billed) AS total_bytes_billed

        FROM {{ ref('raw_gcp_bigquery_cost_allocated_v1') }}
        WHERE allocated_cost_job > 0

        {% if is_incremental() %}
            AND invoice_month_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL {{ lookback_months }} MONTH), MONTH)
        {% endif %}

        GROUP BY project_id, invoice_month_date
    ),

final AS (
        SELECT
            b.invoice_month_date,
            b.project_id,
            COALESCE(d.orgao, 'NÃO DEFINIDO') AS orgao,
            COALESCE(d.ambiente, 'prod') AS ambiente,
            b.service_description,
            b.cost_gross,
            b.credits,
            b.cost_net,
            b.usage_amount,
            CASE WHEN b.service_description = 'BigQuery' THEN s.active_users_count END AS active_users_count,
            CASE WHEN b.service_description = 'BigQuery' THEN s.active_service_accounts_count END AS active_service_accounts_count,
            CASE WHEN b.service_description = 'BigQuery' THEN s.jobs_count END AS jobs_count,
            CASE WHEN b.service_description = 'BigQuery' THEN s.total_bytes_billed END AS total_bytes_billed

        FROM billing_monthly b
        LEFT JOIN {{ ref('raw_dim_gcp_project') }} d
            ON b.project_id = d.project_id
        LEFT JOIN bigquery_stats s
            ON b.project_id = s.project_id
            AND b.invoice_month_date = s.invoice_month_date
            AND b.service_description = 'BigQuery'
    )

SELECT
    invoice_month_date,
    project_id,
    orgao,
    ambiente,
    service_description,
    cost_gross,
    credits,
    cost_net,
    usage_amount,
    active_users_count,
    active_service_accounts_count,
    jobs_count,
    total_bytes_billed
FROM final

{% if is_incremental() %}
WHERE invoice_month_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL {{ lookback_months }} MONTH), MONTH)
{% endif %}
