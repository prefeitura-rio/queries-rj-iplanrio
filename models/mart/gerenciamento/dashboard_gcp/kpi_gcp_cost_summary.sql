{{
    config(
        materialized='table',
        alias='kpi_gcp_cost_summary',
        tags=['dashboard', 'gcp', 'kpi', 'summary'],
    )
}}

-- View de KPIs pré-calculados para dashboard (Overview Executivo)
-- Inclui: total cost, MoM, forecast, tendência, cost per user
-- Pré-calcula métricas complexas para performance no Looker Studio

WITH monthly_costs AS (
    -- Custos mensais totais agregados
    SELECT
        invoice_month_date,
        SUM(cost_net) AS total_cost_net,
        SUM(CASE WHEN service_description = 'BigQuery' THEN cost_net ELSE 0 END) AS bigquery_cost_net,

        -- Economias (conforme console GCP)
        SUM(cud_credits) AS total_cud_credits,
        SUM(other_credits) AS total_other_credits,
        SUM(negotiated_savings) AS total_negotiated_savings,
        SUM(cud_savings) AS total_cud_savings,
        SUM(cud_fee_cost) AS total_cud_fee_cost,

        -- Custo base para comparação
        SUM(cost_at_list) AS total_cost_at_list
    FROM {{ ref('fact_gcp_cost_monthly') }}
    GROUP BY invoice_month_date
),

bigquery_monthly_stats AS (
    -- Estatísticas BigQuery mensais únicas (sem duplicação por projeto)
    -- COUNT DISTINCT direto da fonte raw para evitar duplicação de usuários entre projetos
    SELECT
        invoice_month_date,
        COUNT(DISTINCT CASE WHEN principal_type = 'human' THEN principal_email END) AS total_active_users,
        COUNT(DISTINCT CASE WHEN is_service_account THEN principal_email END) AS total_service_accounts,
        COUNT(DISTINCT job_id) AS total_jobs
    FROM {{ ref('raw_gcp_bigquery_cost_allocated_v1') }}
    WHERE 1=1
    GROUP BY invoice_month_date
),

monthly_combined AS (
    -- Combinar custos e estatísticas BigQuery
    SELECT
        c.invoice_month_date,
        c.total_cost_net,
        c.bigquery_cost_net,
        c.total_cud_credits,
        c.total_other_credits,
        c.total_negotiated_savings,
        c.total_cud_savings,
        c.total_cud_fee_cost,
        c.total_cost_at_list,
        COALESCE(b.total_active_users, 0) AS total_active_users,
        COALESCE(b.total_service_accounts, 0) AS total_service_accounts,
        COALESCE(b.total_jobs, 0) AS total_jobs
    FROM monthly_costs c
    LEFT JOIN bigquery_monthly_stats b
        ON c.invoice_month_date = b.invoice_month_date
),

with_lag AS (
    -- Adicionar valores do mês anterior e médias móveis
    SELECT
        invoice_month_date,
        total_cost_net,
        bigquery_cost_net,
        total_cud_credits,
        total_other_credits,
        total_negotiated_savings,
        total_cud_savings,
        total_cud_fee_cost,
        total_cost_at_list,
        total_active_users,
        total_service_accounts,
        total_jobs,

        -- Valores do mês anterior (LAG)
        LAG(total_cost_net) OVER (ORDER BY invoice_month_date) AS previous_month_total_cost,
        LAG(bigquery_cost_net) OVER (ORDER BY invoice_month_date) AS previous_month_bigquery_cost,
        LAG(total_active_users) OVER (ORDER BY invoice_month_date) AS previous_month_users,
        LAG(total_service_accounts) OVER (ORDER BY invoice_month_date) AS previous_month_service_accounts,

        -- Média móvel 3 meses para forecast
        AVG(total_cost_net) OVER (
            ORDER BY invoice_month_date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS moving_avg_3m_total,

        AVG(bigquery_cost_net) OVER (
            ORDER BY invoice_month_date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS moving_avg_3m_bigquery

    FROM monthly_combined
),

with_metrics AS (
    -- Calcular MoM, tendências e forecasts
    SELECT
        invoice_month_date,
        total_cost_net,
        bigquery_cost_net,
        total_cud_credits,
        total_other_credits,
        total_negotiated_savings,
        total_cud_savings,
        total_cud_fee_cost,
        total_cost_at_list,
        total_active_users,
        total_service_accounts,
        total_jobs,
        previous_month_total_cost,
        previous_month_bigquery_cost,
        previous_month_users,
        previous_month_service_accounts,

        -- Total de economias (conforme console GCP)
        -- Nota: créditos são valores negativos, savings são positivos
        -- O resultado representa a economia líquida total (pode ser negativo se houver mais savings que créditos)
        COALESCE(total_cud_credits, 0) + COALESCE(total_other_credits, 0) + COALESCE(total_negotiated_savings, 0) + COALESCE(total_cud_savings, 0) AS total_savings,

        -- MoM (Month-over-Month) percentual
        SAFE_DIVIDE(
            (total_cost_net - previous_month_total_cost),
            previous_month_total_cost
        ) * 100 AS mom_pct_total,

        SAFE_DIVIDE(
            (bigquery_cost_net - previous_month_bigquery_cost),
            previous_month_bigquery_cost
        ) * 100 AS mom_pct_bigquery,

        SAFE_DIVIDE(
            (total_active_users - previous_month_users),
            previous_month_users
        ) * 100 AS mom_pct_users,

        -- Forecast (média móvel 3 meses)
        moving_avg_3m_total AS forecast_next_month_total,
        moving_avg_3m_bigquery AS forecast_next_month_bigquery,

        -- Tendência (UP/DOWN/STABLE com threshold de ±5%)
        CASE
            WHEN SAFE_DIVIDE((total_cost_net - previous_month_total_cost), previous_month_total_cost) > 0.05 THEN 'UP'
            WHEN SAFE_DIVIDE((total_cost_net - previous_month_total_cost), previous_month_total_cost) < -0.05 THEN 'DOWN'
            ELSE 'STABLE'
        END AS trend_direction,

        -- Custo por usuário (eficiência)
        SAFE_DIVIDE(bigquery_cost_net, NULLIF(total_active_users, 0)) AS cost_per_user,

        -- Flags de período
        invoice_month_date = DATE_TRUNC(CURRENT_DATE(), MONTH) AS is_current_month,
        invoice_month_date = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AS is_previous_month

    FROM with_lag
)

SELECT
    invoice_month_date,

    -- Custos
    total_cost_net,
    bigquery_cost_net,
    total_cost_at_list,

    -- Economias (conforme console GCP)
    total_cud_credits,
    total_other_credits,
    total_negotiated_savings,
    total_cud_savings,
    total_cud_fee_cost,
    total_savings,

    -- Estatísticas
    total_active_users,
    total_service_accounts,
    total_jobs,

    -- Comparações com mês anterior
    previous_month_total_cost,
    previous_month_bigquery_cost,
    previous_month_users,
    previous_month_service_accounts,

    -- MoM (Month-over-Month)
    mom_pct_total,
    mom_pct_bigquery,
    mom_pct_users,

    -- Forecast
    forecast_next_month_total,
    forecast_next_month_bigquery,

    -- Tendências e métricas
    trend_direction,
    cost_per_user,

    -- Flags
    is_current_month,
    is_previous_month
FROM with_metrics
ORDER BY invoice_month_date DESC
