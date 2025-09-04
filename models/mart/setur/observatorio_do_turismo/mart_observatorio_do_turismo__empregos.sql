{{ config(
    materialized='table',
    schema='turismo_fluxo_visitantes',
    alias='empregos',
) }}

WITH empregos_turismo AS (
  SELECT
    SAFE_CAST(mes_ano AS DATE FORMAT "DD/MM/YYYY") as data,
    SAFE_CAST(SAFE_CAST(saldo as FLOAT64) as INT64) as metrica_valor,
    "numero_empregos" as metrica_tipo,
    "Empregos no turismo" as descricao
  FROM {{ ref('raw_oferta_turistica__empregos') }}
),

compara_mes_passado AS (
  SELECT 
    *,
    CASE 
      WHEN metrica_valor IS NULL OR LAG(metrica_valor,1) OVER (PARTITION BY descricao ORDER BY data) IS NULL OR LAG(metrica_valor,1) OVER (PARTITION BY descricao ORDER BY data) = 0
      THEN NULL
      ELSE ROUND(metrica_valor/(LAG(metrica_valor,1) OVER (PARTITION BY descricao ORDER BY data)),2)
    END as variacao_mensal 
  FROM empregos_turismo
  WHERE data <= CURRENT_DATE()
),

data_referencia_dashboard AS ( -- a ULTIMA data em que se tem dados de visitacoes para TODOS os pontos turisticos
  WITH pt_ultima_atualizacao AS (
    SELECT
      descricao,
      MAX(data) data_atualizacao
    FROM compara_mes_passado 
    WHERE metrica_valor IS NOT NULL AND metrica_valor != 0
    GROUP BY descricao
  )
    SELECT
      MIN(data_atualizacao) as data_atualizacao,
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
from compara_mes_passado
left join data_referencia_dashboard ON TRUE
ORDER BY descricao, data