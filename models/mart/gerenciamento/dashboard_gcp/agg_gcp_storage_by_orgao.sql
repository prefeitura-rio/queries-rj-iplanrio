{{
    config(
        materialized='view',
        alias='agg_gcp_storage_by_orgao',
        tags=['dashboard', 'gcp', 'aggregation', 'storage', 'orgao'],
    )
}}

-- View de agregação de armazenamento BigQuery por órgão
-- Inclui: total TB, crescimento, tendências, distribuição por tipo de storage
-- Otimizada para visualização no dashboard de custos GCP

WITH latest_snapshot AS (
    -- Pegar apenas o snapshot mais recente
    SELECT MAX(snapshot_date) AS max_date
    FROM {{ ref('fact_gcp_bigquery_storage') }}
),

storage_by_orgao AS (
    -- Agregação por órgão no snapshot mais recente
    SELECT
        s.snapshot_date,
        s.orgao,

        -- Total de armazenamento
        SUM(s.total_tb) AS total_tb,
        SUM(s.total_gb) AS total_gb,
        SUM(s.total_logical_bytes) AS total_logical_bytes,

        -- Breakdown por tipo de storage
        SUM(s.active_tb) AS active_tb,
        SUM(s.long_term_tb) AS long_term_tb,
        SUM(s.active_logical_bytes) AS active_logical_bytes,
        SUM(s.long_term_logical_bytes) AS long_term_logical_bytes,

        -- Contadores
        SUM(s.dataset_count) AS total_datasets,
        SUM(s.table_count) AS total_tables,
        COUNT(DISTINCT s.project_id) AS project_count,

        -- Crescimento agregado
        SUM(s.growth_bytes) AS growth_bytes,
        SUM(s.growth_gb) AS growth_gb

    FROM {{ ref('fact_gcp_bigquery_storage') }} s
    INNER JOIN latest_snapshot ls
        ON s.snapshot_date = ls.max_date
    GROUP BY s.snapshot_date, s.orgao
),

with_totals AS (
    -- Calcular total geral (para % do total)
    SELECT
        o.*,
        SUM(o.total_tb) OVER () AS total_org_tb
    FROM storage_by_orgao o
),

with_previous AS (
    -- Buscar dados do snapshot anterior (mesmo órgão)
    SELECT
        t.*,

        -- % do total de storage
        SAFE_DIVIDE(t.total_tb, t.total_org_tb) * 100 AS percent_of_total,

        -- Proporção active vs long-term
        SAFE_DIVIDE(t.active_tb, t.total_tb) * 100 AS active_pct,
        SAFE_DIVIDE(t.long_term_tb, t.total_tb) * 100 AS long_term_pct,

        -- Buscar total TB do snapshot anterior
        LAG(t.total_tb) OVER (PARTITION BY t.orgao ORDER BY t.snapshot_date) AS previous_total_tb

    FROM with_totals t
),

with_metrics AS (
    -- Calcular métricas de crescimento
    SELECT
        snapshot_date,
        orgao,

        -- Armazenamento total
        total_tb,
        total_gb,
        total_logical_bytes,

        -- Breakdown por tipo
        active_tb,
        long_term_tb,
        active_pct,
        long_term_pct,

        -- Contadores
        total_datasets,
        total_tables,
        project_count,

        -- Participação
        percent_of_total,

        -- Crescimento snapshot-over-snapshot
        growth_bytes,
        growth_gb,
        SAFE_DIVIDE(growth_gb, previous_total_tb * 1024) * 100 AS growth_rate_pct,  -- Converter TB para GB

        -- Tendência de crescimento
        CASE
            WHEN SAFE_DIVIDE(growth_gb, previous_total_tb * 1024) > 0.05 THEN 'UP'
            WHEN SAFE_DIVIDE(growth_gb, previous_total_tb * 1024) < -0.05 THEN 'DOWN'
            ELSE 'STABLE'
        END AS trend_direction,

        -- Flags
        snapshot_date = CURRENT_DATE() AS is_current_snapshot

    FROM with_previous
)

SELECT
    snapshot_date,
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
    growth_bytes,
    growth_gb,
    growth_rate_pct,
    trend_direction,

    -- Flags
    is_current_snapshot

FROM with_metrics
ORDER BY total_tb DESC
