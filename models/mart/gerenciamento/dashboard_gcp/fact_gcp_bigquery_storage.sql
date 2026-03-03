{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "snapshot_date",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by=["orgao", "project_id"],
        alias='fact_gcp_bigquery_storage',
        tags=['dashboard', 'gcp', 'bigquery', 'storage'],
    )
}}

{% set lookback_months = 3 %}

-- Modelo de armazenamento BigQuery baseado em dados de billing
-- Usa SKUs de storage do billing export para calcular volume armazenado
-- Fonte: raw_gcp_billing (BigQuery storage SKUs)

WITH storage_billing AS (
    SELECT
        DATE_TRUNC(DATE(CONCAT(LEFT(invoice.month, 4), '-', RIGHT(invoice.month, 2), '-01')), MONTH) AS snapshot_date,
        project.id AS project_id,

        -- SKU de storage
        sku.id AS sku_id,
        sku.description AS sku_description,

        -- Unidades de uso (geralmente gibibyte month ou byte-seconds)
        usage.unit,
        usage.pricing_unit,

        -- Quantidade total de uso no mês
        SUM(usage.amount) AS usage_amount,
        SUM(usage.amount_in_pricing_units) AS usage_amount_pricing_units,

        -- Custo associado
        SUM(cost) AS storage_cost

    FROM {{ ref('raw_gcp_billing') }}
    WHERE TRUE
        AND service.description = 'BigQuery'
        AND project.id IS NOT NULL
        -- Filtrar apenas SKUs de storage (Active e Long-term)
        AND (
            LOWER(sku.description) LIKE '%active storage%'
            OR LOWER(sku.description) LIKE '%long%term%storage%'
            OR LOWER(sku.description) LIKE '%long term storage%'
            OR LOWER(sku.description) LIKE '%storage%'
        )
        -- Excluir SKUs que não são de storage
        AND LOWER(sku.description) NOT LIKE '%streaming%'
        AND LOWER(sku.description) NOT LIKE '%analysis%'
        AND LOWER(sku.description) NOT LIKE '%insert%'

    {% if is_incremental() %}
        -- Lookback de +1 mês para garantir que temos o mês anterior disponível para LAG
        AND invoice_competencia_particao >= DATE_TRUNC(
            DATE_SUB(CURRENT_DATE(), INTERVAL {{ lookback_months + 1 }} MONTH),
            MONTH
        )
    {% endif %}

    GROUP BY
        snapshot_date,
        project_id,
        sku_id,
        sku_description,
        usage.unit,
        usage.pricing_unit
),

-- Classificar storage em Active vs Long-term baseado na descrição do SKU
storage_classified AS (
    SELECT
        snapshot_date,
        project_id,

        -- Classificação de tipo de storage
        CASE
            WHEN LOWER(sku_description) LIKE '%long%term%' THEN 'long_term'
            WHEN LOWER(sku_description) LIKE '%active%' THEN 'active'
            ELSE 'other'
        END AS storage_type,

        -- Conversão de unidades para bytes
        -- BigQuery billing pode usar:
        -- - "gibibyte month" (GiB-month): 1 GiB = 1024^3 bytes
        -- - "byte-seconds": precisa converter para bytes mensais
        CASE
            -- Se unidade é gibibyte month, converter para bytes
            WHEN LOWER(pricing_unit) LIKE '%gibibyte%' OR LOWER(unit) LIKE '%gibibyte%'
                THEN usage_amount_pricing_units * POWER(1024, 3)

            -- Se unidade é byte-seconds, converter para bytes usando dias reais do mês
            -- (byte-seconds / seconds_in_month = average bytes)
            WHEN LOWER(pricing_unit) LIKE '%byte%second%' OR LOWER(unit) LIKE '%byte%second%'
                THEN usage_amount / (
                    DATE_DIFF(
                        DATE_ADD(snapshot_date, INTERVAL 1 MONTH),
                        snapshot_date,
                        DAY
                    ) * 24 * 60 * 60
                )

            -- Se já está em bytes
            WHEN LOWER(pricing_unit) LIKE '%byte%' OR LOWER(unit) LIKE '%byte%'
                THEN usage_amount

            -- Fallback: assumir que é gibibyte month
            ELSE usage_amount_pricing_units * POWER(1024, 3)
        END AS storage_bytes,

        storage_cost,
        sku_description,
        unit,
        pricing_unit

    FROM storage_billing
),

-- Agregar por projeto e mês
project_storage_monthly AS (
    SELECT
        snapshot_date,
        project_id,

        -- Total de bytes por tipo
        SUM(CASE WHEN storage_type = 'active' THEN storage_bytes ELSE 0 END) AS active_logical_bytes,
        SUM(CASE WHEN storage_type = 'long_term' THEN storage_bytes ELSE 0 END) AS long_term_logical_bytes,
        SUM(storage_bytes) AS total_logical_bytes,

        -- Custo por tipo
        SUM(CASE WHEN storage_type = 'active' THEN storage_cost ELSE 0 END) AS active_storage_cost,
        SUM(CASE WHEN storage_type = 'long_term' THEN storage_cost ELSE 0 END) AS long_term_storage_cost,
        SUM(storage_cost) AS total_storage_cost

    FROM storage_classified
    GROUP BY snapshot_date, project_id
),

-- Join com dimensão de projetos
final AS (
    SELECT
        s.snapshot_date,
        s.project_id,
        COALESCE(p.orgao, 'NÃO DEFINIDO') AS orgao,
        COALESCE(p.ambiente, 'prod') AS ambiente,
        COALESCE(p.projeto_base, s.project_id) AS projeto_base,

        -- Bytes em formato numérico
        s.total_logical_bytes,
        s.active_logical_bytes,
        s.long_term_logical_bytes,

        -- Time travel e fail-safe não estão disponíveis via billing
        -- Definir como 0 para manter compatibilidade com YAML
        0 AS time_travel_physical_bytes,
        0 AS fail_safe_physical_bytes,

        -- Conversões para unidades legíveis
        ROUND(s.total_logical_bytes / POWER(1024, 3), 2) AS total_gb,
        ROUND(s.total_logical_bytes / POWER(1024, 4), 4) AS total_tb,
        ROUND(s.active_logical_bytes / POWER(1024, 3), 2) AS active_gb,
        ROUND(s.active_logical_bytes / POWER(1024, 4), 4) AS active_tb,
        ROUND(s.long_term_logical_bytes / POWER(1024, 3), 2) AS long_term_gb,
        ROUND(s.long_term_logical_bytes / POWER(1024, 4), 4) AS long_term_tb,

        -- Contadores (não disponíveis via billing)
        -- Definir como NULL para manter compatibilidade com YAML
        CAST(NULL AS INT64) AS dataset_count,
        CAST(NULL AS INT64) AS table_count,

        -- Custos de storage
        s.total_storage_cost,
        s.active_storage_cost,
        s.long_term_storage_cost,

        -- Cálculo de crescimento (comparado com mês anterior)
        LAG(s.total_logical_bytes) OVER (
            PARTITION BY s.project_id
            ORDER BY s.snapshot_date
        ) AS previous_total_bytes,

        -- Growth rate percentual
        SAFE_DIVIDE(
            s.total_logical_bytes - LAG(s.total_logical_bytes) OVER (
                PARTITION BY s.project_id
                ORDER BY s.snapshot_date
            ),
            LAG(s.total_logical_bytes) OVER (
                PARTITION BY s.project_id
                ORDER BY s.snapshot_date
            )
        ) * 100 AS growth_rate_pct

    FROM project_storage_monthly s
    LEFT JOIN {{ ref('raw_dim_gcp_project') }} p
        ON s.project_id = p.project_id
)

SELECT
    snapshot_date,
    project_id,
    orgao,
    ambiente,
    projeto_base,

    -- Bytes brutos
    total_logical_bytes,
    active_logical_bytes,
    long_term_logical_bytes,
    time_travel_physical_bytes,
    fail_safe_physical_bytes,

    -- Unidades legíveis (GB e TB)
    total_gb,
    total_tb,
    active_gb,
    active_tb,
    long_term_gb,
    long_term_tb,

    -- Contadores
    dataset_count,
    table_count,

    -- Custos de storage
    total_storage_cost,
    active_storage_cost,
    long_term_storage_cost,

    -- Crescimento
    previous_total_bytes,
    COALESCE(total_logical_bytes - previous_total_bytes, 0) AS growth_bytes,
    ROUND(COALESCE(total_logical_bytes - previous_total_bytes, 0) / POWER(1024, 3), 2) AS growth_gb,
    growth_rate_pct

FROM final

{% if is_incremental() %}
WHERE snapshot_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL {{ lookback_months }} MONTH), MONTH)
{% endif %}
