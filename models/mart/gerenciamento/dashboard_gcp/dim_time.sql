{{
    config(
        materialized='view',
        schema='dashboard_gcp',
        alias='dim_time',
        tags=['dashboard', 'gcp', 'dimension', 'time'],
    )
}}

-- Dimensão temporal mensal para filtros e agrupamentos
-- Gera calendário desde o primeiro billing até 6 meses no futuro

WITH date_range AS (
    -- Gerar range de meses
    SELECT
        month_date AS invoice_month_date
    FROM UNNEST(
        GENERATE_DATE_ARRAY(
            (SELECT MIN(DATE(CONCAT(LEFT(invoice.month, 4), '-', RIGHT(invoice.month, 2), '-01')))
             FROM {{ ref('raw_gcp_billing') }}),
            DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 6 MONTH),
            INTERVAL 1 MONTH
        )
    ) AS month_date
)

SELECT
    invoice_month_date,

    -- Componentes de data
    EXTRACT(YEAR FROM invoice_month_date) AS year,
    EXTRACT(MONTH FROM invoice_month_date) AS month,
    EXTRACT(QUARTER FROM invoice_month_date) AS quarter,

    -- Formatações
    FORMAT_DATE('%Y-%m', invoice_month_date) AS year_month_name,  -- "2024-01"
    FORMAT_DATE('%B %Y', invoice_month_date) AS month_name_full,  -- "January 2024"
    FORMAT_DATE('%b %Y', invoice_month_date) AS month_name_short, -- "Jan 2024"

    -- Flags de período
    invoice_month_date = DATE_TRUNC(CURRENT_DATE(), MONTH) AS is_current_month,
    invoice_month_date = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AS is_previous_month,
    invoice_month_date < DATE_TRUNC(CURRENT_DATE(), MONTH) AS is_past_month,
    invoice_month_date > DATE_TRUNC(CURRENT_DATE(), MONTH) AS is_future_month,

    -- Sequência para ordenação
    ROW_NUMBER() OVER (ORDER BY invoice_month_date) AS month_sequence

FROM date_range
ORDER BY invoice_month_date
