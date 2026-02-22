{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        schema='brutos_gcp',
        alias='gcp_bigquery_cost_allocated_v1',
        unique_key=['project_id', 'job_id'],
        partition_by={
            "field": "invoice_month_date",
            "data_type": "date",
            "granularity": "month",
        },
        tags=['gcp_billing', 'cost_allocation', 'v1'],
    )
}}

-- Alocação de custo BigQuery por job
-- Une o custo real do billing com os jobs para alocação proporcional baseada em bytes faturados

{% set lookback_months = 3 %}

WITH
    -- CTE 1: Extrair custos do BigQuery por projeto/mês do billing
    billing_bigquery AS (
        SELECT
            project.id AS project_id,

            -- invoice_month_date: conversão de invoice.month (YYYYMM) para DATE
            DATE(CONCAT(LEFT(invoice.month, 4), '-', RIGHT(invoice.month, 2), '-01')) AS invoice_month_date,

            -- Agregações de custo
            SUM(cost) AS bigquery_cost_gross,

            -- Credits: soma do array credits.amount (valores negativos)
            SUM(COALESCE((SELECT SUM(c.amount) FROM UNNEST(credits) AS c), 0)) AS bigquery_credits,

            -- Net: gross + credits (credits são negativos, então é subtração efetiva)
            SUM(cost) + SUM(COALESCE((SELECT SUM(c.amount) FROM UNNEST(credits) AS c), 0)) AS bigquery_cost_net

        FROM {{ ref('raw_gcp_billing') }}
        WHERE service.description = 'BigQuery'
            AND cost_type = 'regular'  -- Excluir tax, adjustment, rounding error

        {% if is_incremental() %}
            -- Incremental: reprocessar últimos N meses para capturar ajustes retroativos
            AND invoice_competencia_particao >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL {{ lookback_months }} MONTH), MONTH)
        {% endif %}

        GROUP BY project_id, invoice_month_date
    ),

    -- CTE 2: Agregar bytes faturados por projeto/mês dos jobs
    jobs_monthly AS (
        SELECT
            project_id,
            job_month_utc,
            SUM(total_bytes_billed) AS total_bytes_project_month
        FROM {{ ref('raw_gcp_bigquery_jobs_v2') }}
        WHERE state = 'DONE'
            AND total_bytes_billed > 0

        {% if is_incremental() %}
            AND job_month_utc >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL {{ lookback_months }} MONTH), MONTH)
        {% endif %}

        GROUP BY project_id, job_month_utc
    ),

    -- CTE 3: Jobs individuais com filtros
    jobs_detail AS (
        SELECT
            project_id,
            job_id,
            principal_email,
            principal_type,
            is_service_account,
            orgao,
            ambiente,
            projeto_base,
            user_email,
            job_type,
            statement_type,
            creation_time,
            end_time,
            job_date_utc,
            job_month_utc,
            total_bytes_billed
        FROM {{ ref('raw_gcp_bigquery_jobs_v2') }}
        WHERE state = 'DONE'
            AND total_bytes_billed > 0

        {% if is_incremental() %}
            AND job_month_utc >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL {{ lookback_months }} MONTH), MONTH)
        {% endif %}
    ),

    -- CTE 4: Join jobs com agregação mensal de bytes
    jobs_with_monthly_total AS (
        SELECT
            jobs.*,
            monthly.total_bytes_project_month
        FROM jobs_detail jobs
        INNER JOIN jobs_monthly monthly
            ON jobs.project_id = monthly.project_id
            AND jobs.job_month_utc = monthly.job_month_utc
    ),

    -- CTE 5: Join com billing e cálculo de alocação
    jobs_with_billing AS (
        SELECT
            jobs.project_id,
            jobs.job_id,
            jobs.principal_email,
            jobs.principal_type,
            jobs.is_service_account,
            jobs.orgao,
            jobs.ambiente,
            jobs.projeto_base,
            jobs.user_email,
            jobs.job_type,
            jobs.statement_type,
            jobs.creation_time,
            jobs.end_time,

            -- Calendário
            jobs.job_date_utc,
            jobs.job_month_utc,
            jobs.job_month_utc AS invoice_month_date,  -- SEMPRE presente (baseado no job)
            billing.invoice_month_date AS billing_invoice_month_date,  -- Opcional (quando há match no billing)

            -- Bytes
            jobs.total_bytes_billed,
            jobs.total_bytes_project_month,

            -- Custos do billing
            COALESCE(billing.bigquery_cost_gross, 0) AS bigquery_cost_gross,
            COALESCE(billing.bigquery_credits, 0) AS bigquery_credits,
            COALESCE(billing.bigquery_cost_net, 0) AS bigquery_cost_net,

            -- Alocação proporcional do custo
            SAFE_DIVIDE(
                jobs.total_bytes_billed * COALESCE(billing.bigquery_cost_net, 0),
                jobs.total_bytes_project_month
            ) AS allocated_cost_job,

            -- Proporção do job no total do projeto/mês
            SAFE_DIVIDE(
                jobs.total_bytes_billed,
                jobs.total_bytes_project_month
            ) AS job_cost_proportion

        FROM jobs_with_monthly_total jobs
        LEFT JOIN billing_bigquery billing
            ON jobs.project_id = billing.project_id
            AND jobs.job_month_utc = billing.invoice_month_date
    )

SELECT
    project_id,
    job_id,
    principal_email,
    principal_type,
    is_service_account,
    orgao,
    ambiente,
    projeto_base,
    user_email,
    job_type,
    statement_type,
    creation_time,
    end_time,
    job_date_utc,
    job_month_utc,
    invoice_month_date,
    billing_invoice_month_date,
    total_bytes_billed,
    total_bytes_project_month,
    bigquery_cost_gross,
    bigquery_credits,
    bigquery_cost_net,
    allocated_cost_job,
    job_cost_proportion
FROM jobs_with_billing

{% if is_incremental() %}
WHERE invoice_month_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL {{ lookback_months }} MONTH), MONTH)
{% endif %}
