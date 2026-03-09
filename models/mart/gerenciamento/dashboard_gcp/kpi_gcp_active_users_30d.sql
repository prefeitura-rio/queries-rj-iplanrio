{{
    config(
        materialized='table',
        alias='kpi_gcp_active_users_30d',
        tags=['dashboard', 'gcp', 'kpi', 'users'],
    )
}}

-- KPI de usuários ativos nos últimos 30 dias com comparação ao período anterior
-- Atualiza diariamente para fornecer métrica sempre atualizada
-- Útil para cards de métricas em tempo real

WITH date_ranges AS (
    -- Define os períodos de análise
    SELECT
        CURRENT_DATE() AS current_date,
        DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AS period_start,
        DATE_SUB(CURRENT_DATE(), INTERVAL 31 DAY) AS previous_period_end,
        DATE_SUB(CURRENT_DATE(), INTERVAL 61 DAY) AS previous_period_start
),

current_period_users AS (
    -- Usuários ativos nos últimos 30 dias (período atual)
    SELECT
        COUNT(DISTINCT CASE WHEN principal_type = 'user' THEN principal_email END) AS active_users,
        COUNT(DISTINCT CASE WHEN is_service_account THEN principal_email END) AS active_service_accounts,
        COUNT(DISTINCT principal_email) AS total_principals,
        COUNT(DISTINCT job_id) AS total_jobs,
        SUM(allocated_cost_job) AS total_cost,
        MIN(creation_time) AS first_activity,
        MAX(creation_time) AS last_activity
    FROM {{ ref('raw_gcp_bigquery_cost_allocated_v1') }} j
    CROSS JOIN date_ranges d
    WHERE DATE(j.creation_time) >= d.period_start
        AND DATE(j.creation_time) < d.current_date
),

previous_period_users AS (
    -- Usuários ativos no período anterior (31-61 dias atrás)
    SELECT
        COUNT(DISTINCT CASE WHEN principal_type = 'user' THEN principal_email END) AS active_users,
        COUNT(DISTINCT CASE WHEN is_service_account THEN principal_email END) AS active_service_accounts,
        COUNT(DISTINCT principal_email) AS total_principals,
        COUNT(DISTINCT job_id) AS total_jobs,
        SUM(allocated_cost_job) AS total_cost
    FROM {{ ref('raw_gcp_bigquery_cost_allocated_v1') }} j
    CROSS JOIN date_ranges d
    WHERE DATE(j.creation_time) >= d.previous_period_start
        AND DATE(j.creation_time) <= d.previous_period_end
),

comparison AS (
    -- Combinar períodos e calcular diferenças
    SELECT
        -- Período de referência
        (SELECT current_date FROM date_ranges) AS reference_date,
        (SELECT period_start FROM date_ranges) AS period_start,
        (SELECT current_date FROM date_ranges) AS period_end,
        30 AS period_days,

        -- Período anterior
        (SELECT previous_period_start FROM date_ranges) AS previous_period_start,
        (SELECT previous_period_end FROM date_ranges) AS previous_period_end,

        -- Métricas do período atual
        c.active_users AS current_active_users,
        c.active_service_accounts AS current_active_service_accounts,
        c.total_principals AS current_total_principals,
        c.total_jobs AS current_total_jobs,
        c.total_cost AS current_total_cost,
        c.first_activity AS current_first_activity,
        c.last_activity AS current_last_activity,

        -- Métricas do período anterior
        p.active_users AS previous_active_users,
        p.active_service_accounts AS previous_active_service_accounts,
        p.total_principals AS previous_total_principals,
        p.total_jobs AS previous_total_jobs,
        p.total_cost AS previous_total_cost,

        -- Variações absolutas
        c.active_users - p.active_users AS users_change,
        c.active_service_accounts - p.active_service_accounts AS service_accounts_change,
        c.total_jobs - p.total_jobs AS jobs_change,
        c.total_cost - p.total_cost AS cost_change,

        -- Variações percentuais
        SAFE_DIVIDE(
            CAST(c.active_users - p.active_users AS FLOAT64),
            CAST(p.active_users AS FLOAT64)
        ) * 100 AS users_change_pct,

        SAFE_DIVIDE(
            CAST(c.active_service_accounts - p.active_service_accounts AS FLOAT64),
            CAST(p.active_service_accounts AS FLOAT64)
        ) * 100 AS service_accounts_change_pct,

        SAFE_DIVIDE(
            CAST(c.total_jobs - p.total_jobs AS FLOAT64),
            CAST(p.total_jobs AS FLOAT64)
        ) * 100 AS jobs_change_pct,

        SAFE_DIVIDE(c.total_cost - p.total_cost, p.total_cost) * 100 AS cost_change_pct,

        -- Métricas derivadas
        SAFE_DIVIDE(c.total_cost, c.active_users) AS cost_per_user,
        SAFE_DIVIDE(c.total_jobs, c.active_users) AS jobs_per_user,
        SAFE_DIVIDE(p.total_cost, p.active_users) AS previous_cost_per_user,

        -- Tendência (UP/DOWN/STABLE com threshold de ±5%)
        CASE
            WHEN SAFE_DIVIDE(CAST(c.active_users - p.active_users AS FLOAT64), CAST(p.active_users AS FLOAT64)) > 0.05 THEN 'UP'
            WHEN SAFE_DIVIDE(CAST(c.active_users - p.active_users AS FLOAT64), CAST(p.active_users AS FLOAT64)) < -0.05 THEN 'DOWN'
            ELSE 'STABLE'
        END AS trend

    FROM current_period_users c
    CROSS JOIN previous_period_users p
)

SELECT
    reference_date,
    period_start,
    period_end,
    period_days,
    previous_period_start,
    previous_period_end,

    -- Usuários ativos (período atual)
    current_active_users,
    current_active_service_accounts,
    current_total_principals,

    -- Atividade (período atual)
    current_total_jobs,
    ROUND(current_total_cost, 2) AS current_total_cost,
    current_first_activity,
    current_last_activity,

    -- Período anterior
    previous_active_users,
    previous_active_service_accounts,
    previous_total_principals,
    previous_total_jobs,
    ROUND(previous_total_cost, 2) AS previous_total_cost,

    -- Variações absolutas
    users_change,
    service_accounts_change,
    jobs_change,
    ROUND(cost_change, 2) AS cost_change,

    -- Variações percentuais
    ROUND(users_change_pct, 2) AS users_change_pct,
    ROUND(service_accounts_change_pct, 2) AS service_accounts_change_pct,
    ROUND(jobs_change_pct, 2) AS jobs_change_pct,
    ROUND(cost_change_pct, 2) AS cost_change_pct,

    -- Eficiência
    ROUND(cost_per_user, 2) AS cost_per_user,
    ROUND(jobs_per_user, 2) AS jobs_per_user,
    ROUND(previous_cost_per_user, 2) AS previous_cost_per_user,

    -- Tendência
    trend

FROM comparison
