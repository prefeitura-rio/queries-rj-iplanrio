{{ config(
    materialized='table',
    schema='turismo_fluxo_visitantes',
    alias='aeroportos',
) }}

WITH aeroportos AS (
SELECT
  SAFE_CAST(mes_ano AS DATE FORMAT "DD/MM/YYYY") as data,
  "Galeão" as nome_aeroporto,
  SAFE_CAST(SAFE_CAST(passageiros_domesticos AS FLOAT64) AS INT64) as passageiros_domesticos,
  SAFE_CAST(SAFE_CAST(passageiros_internacionais AS FLOAT64) AS INT64) as passageiros_internacionais,
  SAFE_CAST(SAFE_CAST(total_de_passageiros AS FLOAT64) AS INT64) as passageiros_total
FROM {{ ref('raw_oferta_turistica__galeao') }}

UNION ALL

SELECT
  SAFE_CAST(mes_ano AS DATE FORMAT "DD/MM/YYYY") as data,
  "Santos Dumont" as nome_aeroporto,
  NULL as passageiros_domesticos,
  NULL as passageiros_internacionais,
  SAFE_CAST(SAFE_CAST(total_de_passageiros AS FLOAT64) AS INT64) as passageiros_total
FROM {{ ref('raw_oferta_turistica__santos_dumont') }}
),

data_referencia_dashboard AS ( -- a ULTIMA data em que se tem dados de visitacoes para TODOS os pontos turisticos
    SELECT
      MAX(data) data_atualizacao
    FROM aeroportos 
    WHERE passageiros_total IS NOT NULL AND passageiros_total != 0
  )

SELECT
  aeroportos.*,
  CASE 
    WHEN data_atualizacao = data THEN TRUE
    ELSE FALSE
    END data_referencia_dashboard
from aeroportos
left join data_referencia_dashboard ON TRUE
WHERE data <= CURRENT_DATE()
  -- AND (passageiros_domesticos IS NOT NULL OR passageiros_internacionais IS NOT NULL OR passageiros_total IS NOT NULL)
ORDER BY nome_aeroporto, aeroportos.data