{{
    config(
        materialized='view',
        alias='dim_time',
        tags=['dashboard', 'gcp', 'dimension', 'time'],
    )
}}

WITH date_range AS (
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
    EXTRACT(YEAR FROM invoice_month_date) AS year,
    EXTRACT(MONTH FROM invoice_month_date) AS month,
    EXTRACT(QUARTER FROM invoice_month_date) AS quarter,
    FORMAT_DATE('%Y-%m', invoice_month_date) AS year_month_name,
    FORMAT_DATE('%B %Y', invoice_month_date) AS month_name_full,
    FORMAT_DATE('%b %Y', invoice_month_date) AS month_name_short,
    invoice_month_date = DATE_TRUNC(CURRENT_DATE(), MONTH) AS is_current_month,
    invoice_month_date = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AS is_previous_month,
    invoice_month_date < DATE_TRUNC(CURRENT_DATE(), MONTH) AS is_past_month,
    invoice_month_date > DATE_TRUNC(CURRENT_DATE(), MONTH) AS is_future_month,
    ROW_NUMBER() OVER (ORDER BY invoice_month_date) AS month_sequence

FROM date_range
