{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "invoice_month_date",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by=["orgao", "principal_email"],
        alias='fact_gcp_cost_by_user',
        tags=['dashboard', 'gcp', 'cost', 'user'],
    )
}}

{% set lookback_months = 3 %}

-- Modelo agregado de custos BigQuery por usuário
-- Granularidade: usuário x projeto x mês
-- Facilita análises de custo por usuário no Looker Studio

WITH user_costs AS (
    SELECT
        invoice_month_date,
        project_id,
        principal_email,
        principal_type,
        is_service_account,

        -- Agregações de custo
        SUM(allocated_cost_job) AS total_cost,
        COUNT(DISTINCT job_id) AS jobs_count,
        SUM(total_bytes_billed) AS total_bytes_billed,

        -- Métricas derivadas
        ROUND(SUM(total_bytes_billed) / POWER(1024, 4), 4) AS total_tib_billed,
        AVG(allocated_cost_job) AS avg_cost_per_job,

        -- Classificação de volume de uso
        CASE
            WHEN SUM(allocated_cost_job) >= 1000 THEN 'Alto (>= R$ 1.000)'
            WHEN SUM(allocated_cost_job) >= 100 THEN 'Médio (R$ 100-1.000)'
            WHEN SUM(allocated_cost_job) >= 10 THEN 'Baixo (R$ 10-100)'
            ELSE 'Muito Baixo (< R$ 10)'
        END AS usage_tier,

        -- Primeira e última execução no mês
        MIN(creation_time) AS first_job_date,
        MAX(creation_time) AS last_job_date

    FROM {{ ref('raw_gcp_bigquery_cost_allocated_v1') }}
    WHERE allocated_cost_job > 0
        AND principal_email IS NOT NULL

    {% if is_incremental() %}
        AND invoice_month_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL {{ lookback_months }} MONTH), MONTH)
    {% endif %}

    GROUP BY
        invoice_month_date,
        project_id,
        principal_email,
        principal_type,
        is_service_account
),

with_project_info AS (
    SELECT
        u.*,
        COALESCE(p.orgao, 'NÃO DEFINIDO') AS orgao,
        COALESCE(p.ambiente, 'prod') AS ambiente,
        COALESCE(p.projeto_base, u.project_id) AS projeto_base
    FROM user_costs u
    LEFT JOIN {{ ref('raw_dim_gcp_project') }} p
        ON u.project_id = p.project_id
),

with_rankings AS (
    -- Adicionar rankings de custo por mês
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY invoice_month_date
            ORDER BY total_cost DESC
        ) AS cost_rank_monthly,

        ROW_NUMBER() OVER (
            PARTITION BY invoice_month_date, orgao
            ORDER BY total_cost DESC
        ) AS cost_rank_by_orgao,

        -- Percentil de custo (0-100)
        PERCENT_RANK() OVER (
            PARTITION BY invoice_month_date
            ORDER BY total_cost
        ) * 100 AS cost_percentile

    FROM with_project_info
)

SELECT
    invoice_month_date,
    project_id,
    orgao,
    ambiente,
    projeto_base,
    principal_email,
    principal_type,
    is_service_account,

    -- Custos e uso
    total_cost,
    jobs_count,
    total_bytes_billed,
    total_tib_billed,
    avg_cost_per_job,
    usage_tier,

    -- Rankings e percentis
    cost_rank_monthly,
    cost_rank_by_orgao,
    ROUND(cost_percentile, 2) AS cost_percentile,

    -- Atividade temporal
    first_job_date,
    last_job_date,
    DATE_DIFF(last_job_date, first_job_date, DAY) AS active_days_in_month,

    -- Flags úteis
    principal_type = 'user' AS is_user,
    cost_rank_monthly <= 10 AS is_top_10_spender,
    cost_percentile >= 90 AS is_top_10_percent

FROM with_rankings

{% if is_incremental() %}
WHERE invoice_month_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL {{ lookback_months }} MONTH), MONTH)
{% endif %}
