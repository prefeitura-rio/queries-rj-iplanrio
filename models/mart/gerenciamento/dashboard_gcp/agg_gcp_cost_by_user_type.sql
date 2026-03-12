{{
    config(
        materialized='view',
        alias='agg_gcp_cost_by_user_type',
        tags=['dashboard', 'gcp', 'aggregation', 'user'],
    )
}}

-- View de agregação de custos separando USUÁRIOS HUMANOS de SERVICE ACCOUNTS
-- Propósito: Facilitar comparação entre custos de automação vs uso humano
-- Granularidade: mês x órgão x tipo de principal

WITH user_costs_aggregated AS (
    SELECT
        invoice_month_date,
        orgao,
        ambiente,
        CASE
            WHEN principal_type = 'user' THEN 'User'
            WHEN is_service_account THEN 'Service Account'
            ELSE 'Other'
        END AS principal_category,

        -- Métricas de custo
        SUM(total_cost) AS total_cost,
        AVG(total_cost) AS avg_cost_per_principal,
        MAX(total_cost) AS max_cost_single_principal,

        -- Contagem de principals
        COUNT(DISTINCT principal_email) AS principals_count,

        -- Métricas de uso
        SUM(jobs_count) AS total_jobs,
        AVG(jobs_count) AS avg_jobs_per_principal,
        SUM(total_tib_billed) AS total_tib_billed,

        -- Eficiência
        SAFE_DIVIDE(SUM(total_cost), SUM(total_tib_billed)) AS cost_per_tib,
        SAFE_DIVIDE(SUM(total_cost), SUM(jobs_count)) AS cost_per_job,

        -- Distribuição de uso
        COUNT(DISTINCT CASE WHEN usage_tier = 'Alto (>= R$ 1.000)' THEN principal_email END) AS high_users_count,
        COUNT(DISTINCT CASE WHEN is_top_10_percent THEN principal_email END) AS top_10_percent_count

    FROM {{ ref('fact_gcp_cost_by_user') }}
    GROUP BY
        invoice_month_date,
        orgao,
        ambiente,
        principal_category
),

with_totals AS (
    -- Calcular percentual do total
    SELECT
        u.*,
        SUM(u.total_cost) OVER (PARTITION BY u.invoice_month_date, u.orgao) AS orgao_total_cost,
        SAFE_DIVIDE(
            u.total_cost,
            SUM(u.total_cost) OVER (PARTITION BY u.invoice_month_date, u.orgao)
        ) * 100 AS pct_of_orgao_cost

    FROM user_costs_aggregated u
),

with_lag AS (
    -- Adicionar valores do mês anterior para MoM
    SELECT
        *,
        LAG(total_cost) OVER (
            PARTITION BY orgao, ambiente, principal_category
            ORDER BY invoice_month_date
        ) AS previous_month_cost,

        LAG(principals_count) OVER (
            PARTITION BY orgao, ambiente, principal_category
            ORDER BY invoice_month_date
        ) AS previous_month_principals_count

    FROM with_totals
)

SELECT
    invoice_month_date,
    orgao,
    ambiente,
    principal_category,

    -- Custos
    total_cost,
    avg_cost_per_principal,
    max_cost_single_principal,

    -- Contadores
    principals_count,
    total_jobs,
    avg_jobs_per_principal,
    total_tib_billed,

    -- Eficiência
    ROUND(cost_per_tib, 4) AS cost_per_tib,
    ROUND(cost_per_job, 4) AS cost_per_job,

    -- Distribuição
    high_users_count,
    top_10_percent_count,

    -- Percentuais
    ROUND(pct_of_orgao_cost, 2) AS pct_of_orgao_cost,

    -- Comparação com mês anterior
    previous_month_cost,
    previous_month_principals_count,

    -- MoM (Month-over-Month) percentual
    ROUND(
        SAFE_DIVIDE(
            (total_cost - previous_month_cost),
            previous_month_cost
        ) * 100,
        2
    ) AS mom_pct_cost,

    ROUND(
        SAFE_DIVIDE(
            CAST(principals_count - previous_month_principals_count AS FLOAT64),
            CAST(previous_month_principals_count AS FLOAT64)
        ) * 100,
        2
    ) AS mom_pct_principals

FROM with_lag
ORDER BY invoice_month_date DESC, orgao, principal_category
