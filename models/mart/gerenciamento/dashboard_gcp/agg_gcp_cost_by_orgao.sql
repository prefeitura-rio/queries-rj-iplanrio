{{
    config(
        materialized='view',
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
        SUM(CASE WHEN service_description = 'BigQuery' THEN cost_net ELSE 0 END) AS bigquery_cost_net,

        -- Economias (conforme console GCP)
        SUM(cud_credits) AS cud_credits,
        SUM(other_credits) AS other_credits,
        SUM(negotiated_savings) AS negotiated_savings,
        SUM(cud_savings) AS cud_savings,
        SUM(cud_fee_cost) AS cud_fee_cost,

        -- Custo base
        SUM(cost_at_list) AS cost_at_list
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
        cud_credits,
        other_credits,
        negotiated_savings,
        cud_savings,
        cud_fee_cost,
        cost_at_list,
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
        cud_credits,
        other_credits,
        negotiated_savings,
        cud_savings,
        cud_fee_cost,
        cost_at_list,
        percent_of_total,
        previous_month_cost,

        -- Total de economias (conforme console GCP)
        -- Nota: créditos são valores negativos, savings são positivos
        -- O resultado representa a economia líquida total
        COALESCE(cud_credits, 0) + COALESCE(other_credits, 0) + COALESCE(negotiated_savings, 0) + COALESCE(cud_savings, 0) AS total_savings,

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

SELECT
    invoice_month_date,
    orgao,

    -- Custos
    cost_net,
    bigquery_cost_net,
    cost_at_list,

    -- Economias (conforme console GCP)
    cud_credits,
    other_credits,
    negotiated_savings,
    cud_savings,
    cud_fee_cost,
    total_savings,

    -- Métricas
    percent_of_total,
    previous_month_cost,
    mom_pct,
    trend_direction,

    -- Flags
    is_current_month,
    is_previous_month

FROM with_metrics
ORDER BY invoice_month_date DESC, cost_net DESC
