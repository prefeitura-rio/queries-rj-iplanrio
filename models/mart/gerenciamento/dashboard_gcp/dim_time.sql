{{
    config(
        materialized='view',
        schema='dashboard_gcp',
        alias='dim_time',
        tags=['dashboard', 'gcp', 'dimension', 'time'],
    )
}}

-- Dimensão temporal para dashboard de custos GCP
-- Gera calendário mensal baseado em dados de billing existentes

WITH date_range AS (
    -- Gerar range de datas desde o primeiro mês de billing até 6 meses no futuro
    SELECT
        DATE_TRUNC(MIN(invoice_competencia_particao), MONTH) AS min_date,
        DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 6 MONTH) AS max_date
    FROM {{ ref('raw_gcp_billing') }}
),

months AS (
    -- Gerar série de meses
    SELECT invoice_month_date
    FROM UNNEST(
        GENERATE_DATE_ARRAY(
            (SELECT min_date FROM date_range),
            (SELECT max_date FROM date_range),
            INTERVAL 1 MONTH
        )
    ) AS invoice_month_date
)

SELECT
    invoice_month_date,

    -- Componentes de data
    EXTRACT(YEAR FROM invoice_month_date) AS year,
    EXTRACT(MONTH FROM invoice_month_date) AS month,
    EXTRACT(QUARTER FROM invoice_month_date) AS quarter,

    -- Formatação
    FORMAT_DATE('%Y-%m', invoice_month_date) AS year_month_name,
    FORMAT_DATE('%B %Y', invoice_month_date) AS month_name_full,
    FORMAT_DATE('%b %Y', invoice_month_date) AS month_name_short,

    -- Flags úteis
    invoice_month_date = DATE_TRUNC(CURRENT_DATE(), MONTH) AS is_current_month,
    invoice_month_date = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AS is_previous_month,
    invoice_month_date < DATE_TRUNC(CURRENT_DATE(), MONTH) AS is_past_month,
    invoice_month_date > DATE_TRUNC(CURRENT_DATE(), MONTH) AS is_future_month,

    -- Ordenação
    ROW_NUMBER() OVER (ORDER BY invoice_month_date) AS month_sequence

FROM months
ORDER BY invoice_month_date
