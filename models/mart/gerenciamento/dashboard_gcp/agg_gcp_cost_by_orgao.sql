{{
    config(
        materialized='view',
        schema='dashboard_gcp',
        alias='agg_gcp_cost_by_orgao',
        tags=['dashboard', 'gcp', 'aggregation', 'orgao'],
    )
}}

-- View agregada por órgão para dashboard (Tabela Top Órgãos)
-- Facilita visualização de custos por órgão com MoM e % do total

WITH monthly_by_orgao AS (
    -- Custos mensais por órgão
    SELECT
        invoice_month_date,
        orgao,
        SUM(cost_net) AS cost_net,
        SUM(CASE WHEN service_description = 'BigQuery' THEN cost_net ELSE 0 END) AS bigquery_cost_net
    FROM {{ ref('fact_gcp_cost_monthly') }}
    GROUP BY invoice_month_date, orgao
),

with_totals AS (
    -- Adicionar totais mensais para cálculo de % do total
    SELECT
        m.*,
        SUM(cost_net) OVER (PARTITION BY invoice_month_date) AS total_month_cost,
        LAG(cost_net) OVER (PARTITION BY orgao ORDER BY invoice_month_date) AS previous_month_cost
    FROM monthly_by_orgao m
),

with_metrics AS (
    -- Calcular % do total e MoM
    SELECT
        invoice_month_date,
        orgao,
        ROUND(cost_net, 2) AS cost_net,
        ROUND(bigquery_cost_net, 2) AS bigquery_cost_net,
        ROUND(previous_month_cost, 2) AS previous_month_cost,

        -- % do total
        ROUND(SAFE_DIVIDE(cost_net, total_month_cost) * 100, 2) AS percent_of_total,

        -- MoM
        ROUND(
            SAFE_DIVIDE(
                (cost_net - previous_month_cost),
                previous_month_cost
            ) * 100,
            2
        ) AS mom_pct,

        -- Tendência
        CASE
            WHEN cost_net > previous_month_cost * 1.05 THEN 'UP'
            WHEN cost_net < previous_month_cost * 0.95 THEN 'DOWN'
            ELSE 'STABLE'
        END AS trend_direction

    FROM with_totals
)

SELECT
    invoice_month_date,
    orgao,
    cost_net,
    bigquery_cost_net,
    previous_month_cost,
    percent_of_total,
    mom_pct,
    trend_direction,

    -- Flags
    invoice_month_date = DATE_TRUNC(CURRENT_DATE(), MONTH) AS is_current_month

FROM with_metrics
ORDER BY invoice_month_date DESC, cost_net DESC
