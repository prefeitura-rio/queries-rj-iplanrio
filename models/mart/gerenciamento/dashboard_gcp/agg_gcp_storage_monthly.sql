{{
    config(
        materialized='view',
        alias='agg_gcp_storage_monthly',
        tags=['dashboard', 'gcp', 'aggregation', 'storage', 'monthly'],
    )
}}

-- View de agregação de armazenamento BigQuery por mês
-- Mostra evolução temporal do armazenamento para análise de crescimento
-- Usa último snapshot de cada mês para representar o mês

WITH monthly_snapshots AS (
    -- Pegar último snapshot de cada mês
    SELECT
        DATE_TRUNC(snapshot_date, MONTH) AS storage_month,
        MAX(snapshot_date) AS max_snapshot_date
    FROM {{ ref('fact_gcp_bigquery_storage') }}
    GROUP BY storage_month
),

storage_monthly AS (
    -- Agregação por órgão e mês usando último snapshot do mês
    SELECT
        DATE_TRUNC(s.snapshot_date, MONTH) AS storage_month,
        s.orgao,

        -- Total de armazenamento
        SUM(s.total_tb) AS total_tb,
        SUM(s.total_gb) AS total_gb,
        SUM(s.total_logical_bytes) AS total_logical_bytes,

        -- Breakdown por tipo
        SUM(s.active_tb) AS active_tb,
        SUM(s.long_term_tb) AS long_term_tb,

        -- Contadores
        SUM(s.dataset_count) AS total_datasets,
        SUM(s.table_count) AS total_tables,
        COUNT(DISTINCT s.project_id) AS project_count

    FROM {{ ref('fact_gcp_bigquery_storage') }} s
    INNER JOIN monthly_snapshots ms
        ON DATE_TRUNC(s.snapshot_date, MONTH) = ms.storage_month
        AND s.snapshot_date = ms.max_snapshot_date
    GROUP BY storage_month, s.orgao
),

with_lag AS (
    -- Adicionar valores do mês anterior para cálculo de crescimento
    SELECT
        storage_month,
        orgao,

        -- Armazenamento
        total_tb,
        total_gb,
        total_logical_bytes,

        -- Breakdown
        active_tb,
        long_term_tb,

        -- Proporções
        SAFE_DIVIDE(active_tb, total_tb) * 100 AS active_pct,
        SAFE_DIVIDE(long_term_tb, total_tb) * 100 AS long_term_pct,

        -- Contadores
        total_datasets,
        total_tables,
        project_count,

        -- Mês anterior
        LAG(total_tb) OVER (PARTITION BY orgao ORDER BY storage_month) AS previous_month_tb,
        LAG(total_gb) OVER (PARTITION BY orgao ORDER BY storage_month) AS previous_month_gb

    FROM storage_monthly
),

with_totals AS (
    -- Calcular total mensal (para % do total)
    SELECT
        l.*,
        SUM(l.total_tb) OVER (PARTITION BY l.storage_month) AS total_month_tb
    FROM with_lag l
),

final AS (
    -- Métricas finais
    SELECT
        storage_month,
        orgao,

        -- Armazenamento
        total_tb,
        total_gb,
        total_logical_bytes,

        -- Breakdown
        active_tb,
        long_term_tb,
        active_pct,
        long_term_pct,

        -- Contadores
        total_datasets,
        total_tables,
        project_count,

        -- Participação no total mensal
        SAFE_DIVIDE(total_tb, total_month_tb) * 100 AS percent_of_total,

        -- Crescimento mês-a-mês (MoM)
        COALESCE(total_tb - previous_month_tb, 0) AS mom_growth_tb,
        COALESCE(total_gb - previous_month_gb, 0) AS mom_growth_gb,

        -- Taxa de crescimento MoM
        SAFE_DIVIDE(
            (total_tb - previous_month_tb),
            previous_month_tb
        ) * 100 AS mom_growth_pct,

        -- Tendência MoM
        CASE
            WHEN SAFE_DIVIDE((total_tb - previous_month_tb), previous_month_tb) > 0.05 THEN 'UP'
            WHEN SAFE_DIVIDE((total_tb - previous_month_tb), previous_month_tb) < -0.05 THEN 'DOWN'
            ELSE 'STABLE'
        END AS trend_direction,

        -- Flags de período
        storage_month = DATE_TRUNC(CURRENT_DATE(), MONTH) AS is_current_month,
        storage_month = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AS is_previous_month

    FROM with_totals
)

SELECT
    storage_month,
    orgao,

    -- Armazenamento
    total_tb,
    total_gb,
    total_logical_bytes,

    -- Breakdown
    active_tb,
    long_term_tb,
    active_pct,
    long_term_pct,

    -- Contadores
    total_datasets,
    total_tables,
    project_count,

    -- Métricas
    percent_of_total,
    mom_growth_tb,
    mom_growth_gb,
    mom_growth_pct,
    trend_direction,

    -- Flags
    is_current_month,
    is_previous_month

FROM final
ORDER BY storage_month DESC, total_tb DESC
