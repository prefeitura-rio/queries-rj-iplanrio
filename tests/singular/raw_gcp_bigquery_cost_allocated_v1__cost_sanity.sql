-- Teste de sanidade: soma dos custos alocados deve ser aproximadamente igual ao custo billing
-- Tolerância: 10%
-- Este teste falha se houver projetos/meses com diferença > 10% entre custo alocado e billing
-- Nota: Billing BigQuery inclui componentes não-rateáveis por bytes (storage, reservations)
-- por isso a tolerância é maior que o ideal de 1%

WITH allocated_summary AS (
    SELECT
        project_id,
        invoice_month_date,
        SUM(allocated_cost_job) AS total_allocated_cost
    FROM {{ ref('raw_gcp_bigquery_cost_allocated_v1') }}
    WHERE invoice_month_date IS NOT NULL
    GROUP BY project_id, invoice_month_date
),

billing_summary AS (
    SELECT
        project.id AS project_id,
        DATE(CONCAT(LEFT(invoice.month, 4), '-', RIGHT(invoice.month, 2), '-01')) AS invoice_month_date,
        SUM(cost) + SUM(COALESCE((SELECT SUM(c.amount) FROM UNNEST(credits) AS c), 0)) AS bigquery_cost_net
    FROM {{ ref('raw_gcp_billing') }}
    WHERE service.description = 'BigQuery'
        AND cost_type = 'regular'
    GROUP BY project_id, invoice_month_date
),

comparison AS (
    SELECT
        COALESCE(a.project_id, b.project_id) AS project_id,
        COALESCE(a.invoice_month_date, b.invoice_month_date) AS invoice_month_date,
        COALESCE(a.total_allocated_cost, 0) AS total_allocated_cost,
        COALESCE(b.bigquery_cost_net, 0) AS bigquery_cost_net,
        ABS(COALESCE(a.total_allocated_cost, 0) - COALESCE(b.bigquery_cost_net, 0)) AS absolute_diff,
        SAFE_DIVIDE(
            ABS(COALESCE(a.total_allocated_cost, 0) - COALESCE(b.bigquery_cost_net, 0)),
            COALESCE(b.bigquery_cost_net, 0)
        ) AS percent_diff
    FROM allocated_summary a
    FULL OUTER JOIN billing_summary b
        ON a.project_id = b.project_id
        AND a.invoice_month_date = b.invoice_month_date
    -- Filtrar apenas casos comparáveis:
    WHERE b.bigquery_cost_net IS NOT NULL  -- Deve ter billing
        AND b.bigquery_cost_net > 1  -- Custo significativo (> $1)
        AND a.total_allocated_cost IS NOT NULL  -- Deve ter alocação
        AND a.total_allocated_cost > 0  -- Alocação > 0
)

-- Retorna linhas onde a diferença percentual é > 10%
-- Se o teste passar, nenhuma linha será retornada
SELECT
    project_id,
    invoice_month_date,
    ROUND(total_allocated_cost, 2) AS total_allocated_cost,
    ROUND(bigquery_cost_net, 2) AS bigquery_cost_net,
    ROUND(absolute_diff, 2) AS absolute_diff,
    ROUND(percent_diff * 100, 2) AS percent_diff_pct
FROM comparison
WHERE percent_diff > 0.10  -- Tolerância de 10%
ORDER BY absolute_diff DESC
