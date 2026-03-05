{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "invoice_month_date",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by=["orgao", "service_description"],
        tags=['dashboard', 'gcp', 'cost', 'monthly'],
    )
}}

{% set lookback_months = 3 %}

WITH billing_monthly AS (
        SELECT
            project.id AS project_id,
            DATE(CONCAT(LEFT(invoice.month, 4), '-', RIGHT(invoice.month, 2), '-01')) AS invoice_month_date,
            service.description AS service_description,

            -- Custos base
            SUM(cost) AS cost_gross,
            SUM(IFNULL(cost_at_list, cost)) AS cost_at_list,
            SUM(IFNULL(cost_at_effective_price_default, cost)) AS cost_at_effective_price,

            -- Créditos separados por categoria (conforme console GCP)
            SUM(IFNULL((
                SELECT SUM(CAST(c.amount AS NUMERIC))
                FROM UNNEST(credits) c
                WHERE c.type IN ('COMMITTED_USAGE_DISCOUNT', 'COMMITTED_USAGE_DISCOUNT_DOLLAR_BASE', 'FEE_UTILIZATION_OFFSET')
            ), 0)) AS cud_credits,

            SUM(IFNULL((
                SELECT SUM(CAST(c.amount AS NUMERIC))
                FROM UNNEST(credits) c
                WHERE c.type IN ('CREDIT_TYPE_UNSPECIFIED', 'PROMOTION', 'SUSTAINED_USAGE_DISCOUNT', 'DISCOUNT', 'FREE_TIER', 'SUBSCRIPTION_BENEFIT', 'RESELLER_MARGIN')
            ), 0)) AS other_credits,

            -- Total de créditos (para compatibilidade)
            SUM(COALESCE((SELECT SUM(c.amount) FROM UNNEST(credits) AS c), 0)) AS credits,

            -- CUD Fee (SKUs específicos conforme console GCP)
            -- SKU IDs validados em 2026-03: representam taxas de CUD no billing export
            SUM(IF(sku.id IN ('5515-81A8-03A2', '7424-6E54-5CD0'), cost, 0)) AS cud_fee_cost,

            -- Economias calculadas (valores positivos = economia real)
            -- negotiated_savings: diferença entre preço de tabela e preço negociado
            SUM(IFNULL(cost_at_list, cost) - IFNULL(cost_at_effective_price_default, cost)) AS negotiated_savings,
            -- cud_savings: diferença entre preço negociado e custo final após CUD
            SUM(IFNULL(cost_at_effective_price_default, cost) - cost) AS cud_savings,

            -- Custo líquido (cost + todos os créditos)
            SUM(cost) + SUM(COALESCE((SELECT SUM(c.amount) FROM UNNEST(credits) AS c), 0)) AS cost_net,

            SUM(usage.amount) AS usage_amount

        FROM {{ ref('raw_gcp_billing') }}
        WHERE project.id IS NOT NULL

        {% if is_incremental() %}
            AND invoice_competencia_particao >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL {{ lookback_months }} MONTH), MONTH)
        {% endif %}

        GROUP BY project_id, invoice_month_date, service_description
    ),

bigquery_stats AS (
        SELECT
            project_id,
            invoice_month_date,

            COUNT(DISTINCT CASE WHEN principal_type = 'user' THEN principal_email END) AS active_users_count,
            COUNT(DISTINCT CASE WHEN is_service_account THEN principal_email END) AS active_service_accounts_count,
            COUNT(DISTINCT job_id) AS jobs_count,
            SUM(total_bytes_billed) AS total_bytes_billed

        FROM {{ ref('raw_gcp_bigquery_cost_allocated_v1') }}
        WHERE allocated_cost_job > 0

        {% if is_incremental() %}
            AND invoice_month_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL {{ lookback_months }} MONTH), MONTH)
        {% endif %}

        GROUP BY project_id, invoice_month_date
    ),

final AS (
        SELECT
            b.invoice_month_date,
            b.project_id,
            COALESCE(d.orgao, 'NÃO DEFINIDO') AS orgao,
            COALESCE(d.ambiente, 'prod') AS ambiente,
            b.service_description,

            -- Custos base
            b.cost_gross,
            b.cost_at_list,
            b.cost_at_effective_price,

            -- Créditos
            b.credits,
            b.cud_credits,
            b.other_credits,

            -- Economias
            b.negotiated_savings,
            b.cud_savings,
            b.cud_fee_cost,

            -- Custo líquido
            b.cost_net,

            b.usage_amount,

            -- Estatísticas BigQuery
            CASE WHEN b.service_description = 'BigQuery' THEN s.active_users_count END AS active_users_count,
            CASE WHEN b.service_description = 'BigQuery' THEN s.active_service_accounts_count END AS active_service_accounts_count,
            CASE WHEN b.service_description = 'BigQuery' THEN s.jobs_count END AS jobs_count,
            CASE WHEN b.service_description = 'BigQuery' THEN s.total_bytes_billed END AS total_bytes_billed

        FROM billing_monthly b
        LEFT JOIN {{ ref('raw_dim_gcp_project') }} d
            ON b.project_id = d.project_id
        LEFT JOIN bigquery_stats s
            ON b.project_id = s.project_id
            AND b.invoice_month_date = s.invoice_month_date
            AND b.service_description = 'BigQuery'
    )

SELECT
    invoice_month_date,
    project_id,
    orgao,
    ambiente,
    service_description,

    -- Custos base
    cost_gross,
    cost_at_list,
    cost_at_effective_price,

    -- Créditos
    credits,
    cud_credits,
    other_credits,

    -- Economias
    negotiated_savings,
    cud_savings,
    cud_fee_cost,

    -- Custo líquido
    cost_net,

    usage_amount,

    -- Estatísticas BigQuery
    active_users_count,
    active_service_accounts_count,
    jobs_count,
    total_bytes_billed
FROM final

{% if is_incremental() %}
WHERE invoice_month_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL {{ lookback_months }} MONTH), MONTH)
{% endif %}
