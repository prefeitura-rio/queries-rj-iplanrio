{{ config(
    materialized='table',
    schema='turismo_fluxo_visitantes',
    alias='pontos_turisticos',
) }}

  WITH pontos_turisticos AS (
  SELECT
    SAFE_CAST(mes_ano AS DATE FORMAT "DD/MM/YYYY") as data,
    SAFE_CAST(REPLACE(no_visitantes, ".", "") AS INT64) as metrica_valor,
    "numero_visitantes" as metrica_tipo,
    "AquaRio" as ferramenta_turistica 
  FROM {{ ref('raw_oferta_turistica__aqua_rio') }}

  UNION ALL

  SELECT
    SAFE_CAST(mes_ano AS DATE FORMAT "DD/MM/YYYY") as data,
    SAFE_CAST(REPLACE(REPLACE(visitantes, ".", ""), "-", "") AS INT64) as metrica_valor,
    "numero_visitantes" as metrica_tipo,
    "Bio Parque" as ferramenta_turistica 
  FROM {{ ref('raw_oferta_turistica__bio_parque') }}

  UNION ALL

  SELECT
    SAFE_CAST(mes_ano AS DATE FORMAT "DD/MM/YYYY") as data,
    SAFE_CAST(SAFE_CAST(visitantes as FLOAT64) AS INT64) as metrica_valor,
    "numero_visitantes" as metrica_tipo,
    "CCBB" as ferramenta_turistica 
  FROM {{ ref('raw_oferta_turistica__ccbb') }}

  UNION ALL

  SELECT
    SAFE_CAST(mes_ano AS DATE FORMAT "DD/MM/YYYY") as data,
    SAFE_CAST(REPLACE(REPLACE(no_visitantes, ".", ""), "-", "") AS INT64) as metrica_valor,
    "numero_visitantes" as metrica_tipo,
    "Cristo Redentor" as ferramenta_turistica 
  FROM {{ ref('raw_oferta_turistica__cristo') }}

  UNION ALL

  SELECT
    SAFE_CAST(mes_ano AS DATE FORMAT "DD/MM/YYYY") as data,
    SAFE_CAST(SAFE_CAST(no_visitantes as FLOAT64) as INT64) as metrica_valor,
    "numero_visitantes" as metrica_tipo,
    "Museu do Amanhã" as ferramenta_turistica 
  FROM {{ ref('raw_oferta_turistica__museu_do_amanha') }}

  UNION ALL

  SELECT
    SAFE_CAST(mes_ano AS DATE FORMAT "DD/MM/YYYY") as data,
    SAFE_CAST(SAFE_CAST(visitantes as FLOAT64) as INT64) as metrica_valor,
    "numero_visitantes" as metrica_tipo,
    "Pão de Açúcar" as ferramenta_turistica 
  FROM {{ ref('raw_oferta_turistica__pao_de_acucar') }}

--   UNION ALL

-- SELECT
--   SAFE_CAST(SAFE_CAST(ano_mes AS STRING) AS DATE FORMAT "YYYY-MM-DD") as data,
--   SAFE_CAST(SAFE_CAST(valor_visitacao as FLOAT64) as INT64) as metrica_valor,
--   "numero_visitantes" as metrica_tipo,
--   "Cais do Valongo" as ferramenta_turistica 
-- FROM `datario.povo_comunidades_tradicionais.visita_valongo`
),

compara_mes_passado AS (
  SELECT 
    *,
    CASE 
      WHEN metrica_valor IS NULL OR LAG(metrica_valor,1) OVER (PARTITION BY ferramenta_turistica ORDER BY data) IS NULL OR LAG(metrica_valor,1) OVER (PARTITION BY ferramenta_turistica ORDER BY data) = 0
      THEN NULL
      ELSE ROUND(metrica_valor/(LAG(metrica_valor,1) OVER (PARTITION BY ferramenta_turistica ORDER BY data))- 1,2)
    END as variacao_mensal 
  FROM pontos_turisticos
  WHERE data <= CURRENT_DATE()
),

data_referencia_dashboard AS ( -- a ULTIMA data em que se tem dados de visitacoes para TODOS os pontos turisticos
  WITH pt_ultima_atualizacao AS (
    SELECT
      ferramenta_turistica,
      MAX(data) data_atualizacao
    FROM compara_mes_passado 
    WHERE metrica_valor IS NOT NULL AND metrica_valor != 0
    GROUP BY ferramenta_turistica
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
ORDER BY ferramenta_turistica, data

