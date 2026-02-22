{{
    config(
        materialized='view',
        schema='dashboard_gcp',
        alias='agg_gcp_cost_by_orgao',
        tags=['dashboard', 'gcp', 'aggregation', 'orgao'],
    )
}}

-- View de agregação de custos por órgão/mês
-- Inclui: cost_net, % do total, MoM, tendência
-- Otimizada para Tabela Top Órgãos do dashboard

WITH orgao_monthly AS (
    -- Agregação por órgão e mês
    SELECT
        invoice_month_date,
        orgao,
        SUM(cost_net) AS cost_net,
        SUM(CASE WHEN service_description = 'BigQuery' THEN cost_net ELSE 0 END) AS bigquery_cost_net
    FROM {{ ref('fact_gcp_cost_monthly') }}
    GROUP BY invoice_month_date, orgao
),

with_totals AS (
    -- Calcular total do mês (para % do total)
    SELECT
        o.*,
        SUM(o.cost_net) OVER (PARTITION BY o.invoice_month_date) AS total_month_cost
    FROM orgao_monthly o
),

with_lag AS (
    -- Adicionar valores do mês anterior
    SELECT
        invoice_month_date,
        orgao,
        cost_net,
        bigquery_cost_net,
        total_month_cost,

        -- % do total
        SAFE_DIVIDE(cost_net, total_month_cost) * 100 AS percent_of_total,

        -- Custo do mês anterior
        LAG(cost_net) OVER (PARTITION BY orgao ORDER BY invoice_month_date) AS previous_month_cost

    FROM with_totals
),

with_metrics AS (
    -- Calcular MoM e tendência
    SELECT
        invoice_month_date,
        orgao,
        cost_net,
        bigquery_cost_net,
        percent_of_total,
        previous_month_cost,

        -- MoM percentual
        SAFE_DIVIDE(
            (cost_net - previous_month_cost),
            previous_month_cost
        ) * 100 AS mom_pct,

        -- Tendência (threshold ±5%)
        CASE
            WHEN SAFE_DIVIDE((cost_net - previous_month_cost), previous_month_cost) > 0.05 THEN 'UP'
            WHEN SAFE_DIVIDE((cost_net - previous_month_cost), previous_month_cost) < -0.05 THEN 'DOWN'
            ELSE 'STABLE'
        END AS trend_direction,

        -- Flags de período
        invoice_month_date = DATE_TRUNC(CURRENT_DATE(), MONTH) AS is_current_month,
        invoice_month_date = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AS is_previous_month

    FROM with_lag
)

SELECT * FROM with_metrics
ORDER BY invoice_month_date DESC, cost_net DESC
