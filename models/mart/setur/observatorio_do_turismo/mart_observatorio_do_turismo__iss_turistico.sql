{{ config(
    materialized='table',
    schema='turismo_fluxo_visitantes',
    alias='iss_turistico',
) }}

WITH tratamento_inicial AS (
  SELECT 
    SAFE_CAST(mes_ano AS DATE FORMAT "DD/MM/YYYY") as data,
    CASE
    WHEN IS_NAN(SAFE_CAST(REPLACE(total_arrecadado,",", ".") as FLOAT64)) THEN NULL
    ELSE SAFE_CAST(REPLACE(total_arrecadado,",", ".") as FLOAT64)
    END as total_arrecadado,
FROM {{ ref('raw_oferta_turistica__iss') }}
WHERE SAFE_CAST(mes_ano AS DATE FORMAT "DD/MM/YYYY") <= CURRENT_DATE()
),

total_por_ano AS (
SELECT
  EXTRACT(YEAR FROM data) as ano,
  SUM(total_arrecadado) as total_ano
FROM tratamento_inicial
GROUP BY ano
)

SELECT 
  t.data,
  a.ano,
  t.total_arrecadado,
  ROUND(total_arrecadado/total_ano,4) as percentual_arrecadado_ano
FROM tratamento_inicial as t
LEFT JOIN total_por_ano as a
  ON EXTRACT(YEAR FROM t.data) = a.ano