{{ config(
    materialized='table',
    schema='turismo_fluxo_visitantes',
    alias='claro_nacionalidade',
) }}

WITH nacionalidades_2023 AS (
SELECT 
  CASE 
    WHEN _2023 = "Janeiro" THEN DATE("2023-01-01")
    WHEN _2023 = "fevereiro" THEN DATE("2023-02-01")
    WHEN _2023 = "março" THEN DATE("2023-03-01")
    WHEN _2023 = "abril" THEN DATE("2023-04-01")
    WHEN _2023 = "maio" THEN DATE("2023-05-01")
    WHEN _2023 = "junho" THEN DATE("2023-06-01")
    WHEN _2023 = "julho" THEN DATE("2023-07-01")
    WHEN _2023 = "agosto" THEN DATE("2023-08-01")
    WHEN _2023 = "setembro" THEN DATE("2023-09-01")
    WHEN _2023 = "outubro" THEN DATE("2023-10-01")
    WHEN _2023 = "novembro" THEN DATE("2023-11-01")
    WHEN _2023 = "dezembro" THEN DATE("2023-12-01")
  ELSE NULL
  END AS data,
  SAFE_CAST(SAFE_CAST(REPLACE(REPLACE(metrica_valor, ".", ""),",", ".") AS FLOAT64) AS INT64) as metrica_valor,
  "numero_visitantes" as metrica_tipo,
  origem
FROM {{ ref('raw_turismo_fluxo_visitantes__claro_nacionalidade_2023_clean') }}
UNPIVOT(metrica_valor FOR origem IN (
    internacionais,
    nacionais
    )
  )
),

nacionalidades_2024 AS (
SELECT 
  CASE 
    WHEN _2023 = "Janeiro" THEN DATE("2024-01-01")
    WHEN _2023 = "fevereiro" THEN DATE("2024-02-01")
    WHEN _2023 = "março" THEN DATE("2024-03-01")
    WHEN _2023 = "abril" THEN DATE("2024-04-01")
    WHEN _2023 = "maio" THEN DATE("2024-05-01")
    WHEN _2023 = "junho" THEN DATE("2024-06-01")
    WHEN _2023 = "julho" THEN DATE("2024-07-01")
    WHEN _2023 = "agosto" THEN DATE("2024-08-01")
    WHEN _2023 = "setembro" THEN DATE("2024-09-01")
    WHEN _2023 = "outubro" THEN DATE("2024-10-01")
    WHEN _2023 = "novembro" THEN DATE("2024-11-01")
    WHEN _2023 = "dezembro" THEN DATE("2024-12-01")
  ELSE NULL
  END AS data,
  SAFE_CAST(SAFE_CAST(REPLACE(REPLACE(metrica_valor, ".", ""),",", ".") AS FLOAT64) AS INT64) as metrica_valor,
  "numero_visitantes" as metrica_tipo,
  origem
FROM {{ ref('raw_turismo_fluxo_visitantes__claro_nacionalidade_2024_clean') }}
UNPIVOT(metrica_valor FOR origem IN (
    internacionais,
    nacionais
    )
  )
),

nacionalidades_2025 AS (
SELECT 
  CASE 
    WHEN _2023 = "Janeiro" THEN DATE("2025-01-01")
    WHEN _2023 = "fevereiro" THEN DATE("2025-02-01")
    WHEN _2023 = "março" THEN DATE("2025-03-01")
    WHEN _2023 = "abril" THEN DATE("2025-04-01")
    WHEN _2023 = "maio" THEN DATE("2025-05-01")
    WHEN _2023 = "junho" THEN DATE("2025-06-01")
    WHEN _2023 = "julho" THEN DATE("2025-07-01")
    WHEN _2023 = "agosto" THEN DATE("2025-08-01")
    WHEN _2023 = "setembro" THEN DATE("2025-09-01")
    WHEN _2023 = "outubro" THEN DATE("2025-10-01")
    WHEN _2023 = "novembro" THEN DATE("2025-11-01")
    WHEN _2023 = "dezembro" THEN DATE("2025-12-01")
  ELSE NULL
  END AS data,
  SAFE_CAST(SAFE_CAST(REPLACE(REPLACE(metrica_valor, ".", ""),",", ".") AS FLOAT64) AS INT64) as metrica_valor,
  "numero_visitantes" as metrica_tipo,
  origem
FROM {{ ref('raw_turismo_fluxo_visitantes__claro_nacionalidade_2025_clean') }}
UNPIVOT(metrica_valor FOR origem IN (
    internacionais,
    nacionais
    )
  )
),

nacionalidades AS (
  SELECT * FROM nacionalidades_2023
  
  UNION ALL
  
  SELECT * FROM nacionalidades_2024
  
  UNION ALL
  
  SELECT * FROM nacionalidades_2025
),

compara_mes_passado AS (
  SELECT 
    *,
    CASE 
      WHEN metrica_valor IS NULL OR LAG(metrica_valor,1) OVER (PARTITION BY origem ORDER BY data) IS NULL OR LAG(metrica_valor,1) OVER (PARTITION BY origem ORDER BY data) = 0
      THEN NULL
      ELSE ROUND(metrica_valor/(LAG(metrica_valor,1) OVER (PARTITION BY origem ORDER BY data))- 1,2)
    END as variacao_mensal 
  FROM nacionalidades
  WHERE data <= CURRENT_DATE()
),

data_referencia_dashboard AS ( -- a ULTIMA data em que se tem dados de visitacoes para TODOS os pontos turisticos
  WITH pt_ultima_atualizacao AS (
    SELECT
      origem,
      MAX(data) data_atualizacao
    FROM compara_mes_passado 
    WHERE metrica_valor IS NOT NULL AND metrica_valor != 0
    GROUP BY origem
  )
    SELECT
      MIN(data_atualizacao) as data_atualizacao
    FROM pt_ultima_atualizacao 
)

SELECT
  compara_mes_passado.*,
  CASE 
        WHEN variacao_mensal > 0 THEN CONCAT("↑ +", ROUND(variacao_mensal * 100,0), "%")
        WHEN variacao_mensal < 0 THEN CONCAT("↓ -", ROUND(variacao_mensal * -1 * 100,0), "%")
        ELSE CONCAT("   ", variacao_mensal)
  END variacao_mensal_formatada,
  CASE 
    WHEN data_atualizacao = data THEN TRUE
    ELSE FALSE
    END data_referencia_dashboard
FROM compara_mes_passado
LEFT JOIN data_referencia_dashboard ON TRUE
ORDER BY origem, data