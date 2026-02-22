{{
    config(
        materialized='view',
        schema='dashboard_gcp',
        alias='kpi_gcp_cost_summary',
        tags=['dashboard', 'gcp', 'kpi', 'summary'],
    )
}}

-- View de KPIs calculados para dashboard (Overview Executivo)
-- Inclui: total cost, MoM, forecast, tendência
-- Pré-calcula métricas complexas para performance no Looker Studio

WITH monthly_costs AS (
    -- Custos mensais totais
    SELECT
        invoice_month_date,
        SUM(cost_net) AS total_cost_net,
        SUM(CASE WHEN service_description = 'BigQuery' THEN cost_net ELSE 0 END) AS bigquery_cost_net,
        SUM(CASE WHEN service_description = 'BigQuery' THEN active_users_count ELSE 0 END) AS total_active_users,
        SUM(CASE WHEN service_description = 'BigQuery' THEN jobs_count ELSE 0 END) AS total_jobs
    FROM {{ ref('fact_gcp_cost_monthly') }}
    GROUP BY invoice_month_date
),

with_lag AS (
    -- Adicionar valores do mês anterior
    SELECT
        invoice_month_date,
        total_cost_net,
        bigquery_cost_net,
        total_active_users,
        total_jobs,

        -- Valores do mês anterior
        LAG(total_cost_net) OVER (ORDER BY invoice_month_date) AS previous_month_total_cost,
        LAG(bigquery_cost_net) OVER (ORDER BY invoice_month_date) AS previous_month_bigquery_cost,
        LAG(total_active_users) OVER (ORDER BY invoice_month_date) AS previous_month_users,

        -- Média móvel 3 meses
        AVG(total_cost_net) OVER (
            ORDER BY invoice_month_date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS moving_avg_3m_total,

        AVG(bigquery_cost_net) OVER (
            ORDER BY invoice_month_date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS moving_avg_3m_bigquery

    FROM monthly_costs
),

with_metrics AS (
    -- Calcular variações e tendências
    SELECT
        *,

        -- MoM percentual
        ROUND(
            SAFE_DIVIDE(
                (total_cost_net - previous_month_total_cost),
                previous_month_total_cost
            ) * 100,
            2
        ) AS mom_pct_total,

        ROUND(
            SAFE_DIVIDE(
                (bigquery_cost_net - previous_month_bigquery_cost),
                previous_month_bigquery_cost
            ) * 100,
            2
        ) AS mom_pct_bigquery,

        ROUND(
            SAFE_DIVIDE(
                (total_active_users - previous_month_users),
                previous_month_users
            ) * 100,
            2
        ) AS mom_pct_users,

        -- Tendência (UP/DOWN/STABLE)
        CASE
            WHEN total_cost_net > previous_month_total_cost * 1.05 THEN 'UP'
            WHEN total_cost_net < previous_month_total_cost * 0.95 THEN 'DOWN'
            ELSE 'STABLE'
        END AS trend_direction,

        -- Custo por usuário
        ROUND(SAFE_DIVIDE(bigquery_cost_net, total_active_users), 2) AS cost_per_user

    FROM with_lag
)

SELECT
    invoice_month_date,

    -- Custos
    ROUND(total_cost_net, 2) AS total_cost_net,
    ROUND(bigquery_cost_net, 2) AS bigquery_cost_net,
    ROUND(previous_month_total_cost, 2) AS previous_month_total_cost,
    ROUND(previous_month_bigquery_cost, 2) AS previous_month_bigquery_cost,

    -- Usuários e Jobs
    total_active_users,
    previous_month_users,
    total_jobs,

    -- Variações MoM
    mom_pct_total,
    mom_pct_bigquery,
    mom_pct_users,

    -- Tendência
    trend_direction,

    -- Média móvel (forecast)
    ROUND(moving_avg_3m_total, 2) AS forecast_next_month_total,
    ROUND(moving_avg_3m_bigquery, 2) AS forecast_next_month_bigquery,

    -- Métricas derivadas
    cost_per_user,

    -- Flags úteis
    invoice_month_date = DATE_TRUNC(CURRENT_DATE(), MONTH) AS is_current_month,
    invoice_month_date = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AS is_previous_month

FROM with_metrics
ORDER BY invoice_month_date DESC
