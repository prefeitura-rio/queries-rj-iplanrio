{{
    config(
        materialized='table',
        alias='kpi_gcp_active_users_30d_by_orgao',
        tags=['dashboard', 'gcp', 'kpi', 'users', 'orgao'],
    )
}}

-- KPI de usuários ativos BigQuery nos últimos 30 dias POR ÓRGÃO
-- Permite ranking de órgãos por crescimento de usuários
-- Comparação com período anterior (31-61 dias atrás)

WITH date_ranges AS (
    -- Define os períodos de análise
    SELECT
        CURRENT_DATE() AS current_date,
        DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AS period_start,
        DATE_SUB(CURRENT_DATE(), INTERVAL 31 DAY) AS previous_period_end,
        DATE_SUB(CURRENT_DATE(), INTERVAL 61 DAY) AS previous_period_start
),

current_period_users_by_orgao AS (
    -- Usuários ativos nos últimos 30 dias por órgão
    SELECT
        COALESCE(p.orgao, 'NÃO DEFINIDO') AS orgao,
        COALESCE(p.ambiente, 'prod') AS ambiente,

        COUNT(DISTINCT CASE WHEN j.principal_type = 'human' THEN j.principal_email END) AS active_users,
        COUNT(DISTINCT CASE WHEN j.is_service_account THEN j.principal_email END) AS active_service_accounts,
        COUNT(DISTINCT j.principal_email) AS total_principals,
        COUNT(DISTINCT j.job_id) AS total_jobs,
        SUM(j.allocated_cost_job) AS total_cost,
        MIN(j.creation_time) AS first_activity,
        MAX(j.creation_time) AS last_activity

    FROM {{ ref('raw_gcp_bigquery_cost_allocated_v1') }} j
    CROSS JOIN date_ranges d
    LEFT JOIN {{ ref('raw_dim_gcp_project') }} p
        ON j.project_id = p.project_id
    WHERE DATE(j.creation_time) >= d.period_start
        AND DATE(j.creation_time) < d.current_date
    GROUP BY orgao, ambiente
),

previous_period_users_by_orgao AS (
    -- Usuários ativos no período anterior (30 dias: 31-61 dias atrás) por órgão
    SELECT
        COALESCE(p.orgao, 'NÃO DEFINIDO') AS orgao,
        COALESCE(p.ambiente, 'prod') AS ambiente,

        COUNT(DISTINCT CASE WHEN j.principal_type = 'human' THEN j.principal_email END) AS active_users,
        COUNT(DISTINCT CASE WHEN j.is_service_account THEN j.principal_email END) AS active_service_accounts,
        COUNT(DISTINCT j.principal_email) AS total_principals,
        COUNT(DISTINCT j.job_id) AS total_jobs,
        SUM(j.allocated_cost_job) AS total_cost

    FROM {{ ref('raw_gcp_bigquery_cost_allocated_v1') }} j
    CROSS JOIN date_ranges d
    LEFT JOIN {{ ref('raw_dim_gcp_project') }} p
        ON j.project_id = p.project_id
    WHERE DATE(j.creation_time) >= d.previous_period_start
        AND DATE(j.creation_time) < d.previous_period_end
    GROUP BY orgao, ambiente
),

comparison AS (
    -- Combinar períodos e calcular diferenças por órgão
    SELECT
        -- Identificação (usa COALESCE para capturar órgãos de ambos os períodos)
        COALESCE(c.orgao, p.orgao) AS orgao,
        COALESCE(c.ambiente, p.ambiente) AS ambiente,

        -- Período de referência
        (SELECT current_date FROM date_ranges) AS reference_date,
        (SELECT period_start FROM date_ranges) AS period_start,
        (SELECT current_date FROM date_ranges) AS period_end,
        30 AS period_days,

        -- Métricas do período atual (COALESCE para órgãos que só existem no anterior)
        COALESCE(c.active_users, 0) AS current_active_users,
        COALESCE(c.active_service_accounts, 0) AS current_active_service_accounts,
        COALESCE(c.total_principals, 0) AS current_total_principals,
        COALESCE(c.total_jobs, 0) AS current_total_jobs,
        COALESCE(c.total_cost, 0) AS current_total_cost,
        c.first_activity AS current_first_activity,
        c.last_activity AS current_last_activity,

        -- Métricas do período anterior
        COALESCE(p.active_users, 0) AS previous_active_users,
        COALESCE(p.active_service_accounts, 0) AS previous_active_service_accounts,
        COALESCE(p.total_principals, 0) AS previous_total_principals,
        COALESCE(p.total_jobs, 0) AS previous_total_jobs,
        COALESCE(p.total_cost, 0) AS previous_total_cost,

        -- Variações absolutas
        COALESCE(c.active_users, 0) - COALESCE(p.active_users, 0) AS users_change,
        COALESCE(c.active_service_accounts, 0) - COALESCE(p.active_service_accounts, 0) AS service_accounts_change,
        COALESCE(c.total_jobs, 0) - COALESCE(p.total_jobs, 0) AS jobs_change,
        COALESCE(c.total_cost, 0) - COALESCE(p.total_cost, 0) AS cost_change,

        -- Variações percentuais
        SAFE_DIVIDE(
            CAST(COALESCE(c.active_users, 0) - COALESCE(p.active_users, 0) AS FLOAT64),
            CAST(NULLIF(p.active_users, 0) AS FLOAT64)
        ) * 100 AS users_change_pct,

        SAFE_DIVIDE(
            CAST(COALESCE(c.active_service_accounts, 0) - COALESCE(p.active_service_accounts, 0) AS FLOAT64),
            CAST(NULLIF(p.active_service_accounts, 0) AS FLOAT64)
        ) * 100 AS service_accounts_change_pct,

        SAFE_DIVIDE(
            CAST(COALESCE(c.total_jobs, 0) - COALESCE(p.total_jobs, 0) AS FLOAT64),
            CAST(NULLIF(p.total_jobs, 0) AS FLOAT64)
        ) * 100 AS jobs_change_pct,

        SAFE_DIVIDE(
            COALESCE(c.total_cost, 0) - COALESCE(p.total_cost, 0),
            NULLIF(p.total_cost, 0)
        ) * 100 AS cost_change_pct,

        -- Métricas derivadas
        SAFE_DIVIDE(COALESCE(c.total_cost, 0), NULLIF(c.active_users, 0)) AS cost_per_user,
        SAFE_DIVIDE(CAST(COALESCE(c.total_jobs, 0) AS FLOAT64), CAST(NULLIF(c.active_users, 0) AS FLOAT64)) AS jobs_per_user,
        SAFE_DIVIDE(COALESCE(p.total_cost, 0), NULLIF(p.active_users, 0)) AS previous_cost_per_user,

        -- Tendência (UP/DOWN/STABLE/NEW/INACTIVE com threshold de ±5%)
        CASE
            WHEN c.active_users IS NULL OR c.active_users = 0 THEN 'INACTIVE'  -- Tinha usuários antes, não tem mais
            WHEN p.active_users IS NULL OR p.active_users = 0 THEN 'NEW'       -- Não tinha antes, tem agora
            WHEN SAFE_DIVIDE(CAST(c.active_users - p.active_users AS FLOAT64), CAST(p.active_users AS FLOAT64)) > 0.05 THEN 'UP'
            WHEN SAFE_DIVIDE(CAST(c.active_users - p.active_users AS FLOAT64), CAST(p.active_users AS FLOAT64)) < -0.05 THEN 'DOWN'
            ELSE 'STABLE'
        END AS trend

    FROM current_period_users_by_orgao c
    FULL OUTER JOIN previous_period_users_by_orgao p
        ON c.orgao = p.orgao
        AND c.ambiente = p.ambiente
),

with_rankings AS (
    -- Adicionar rankings de crescimento
    SELECT
        *,
        -- Ranking de crescimento absoluto de usuários
        ROW_NUMBER() OVER (ORDER BY users_change DESC) AS growth_rank_absolute,

        -- Ranking de crescimento percentual de usuários
        ROW_NUMBER() OVER (ORDER BY users_change_pct DESC) AS growth_rank_percentage,

        -- Ranking de usuários ativos (maior para menor)
        ROW_NUMBER() OVER (ORDER BY current_active_users DESC) AS users_rank,

        -- Flag de top crescimento
        users_change_pct >= 20 AS is_high_growth

    FROM comparison
)

SELECT
    orgao,
    ambiente,
    reference_date,
    period_start,
    period_end,
    period_days,

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
    trend,

    -- Rankings
    growth_rank_absolute,
    growth_rank_percentage,
    users_rank,
    is_high_growth

FROM with_rankings
