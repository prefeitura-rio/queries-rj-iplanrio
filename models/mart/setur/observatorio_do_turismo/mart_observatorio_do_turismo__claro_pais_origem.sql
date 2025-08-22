{{ config(
    materialized='table',
    alias='claro_pais_origem',
    schema='turismo_fluxo_visitantes'
) }}

WITH claro_origem_pais_0 as (
  SELECT
  CASE 
    WHEN data = "janeiro" THEN DATE("2023-01-01")
    WHEN data = "fevereiro" THEN DATE("2023-02-01")
    WHEN data = "marco" THEN DATE("2023-03-01")
    WHEN data = "abril" THEN DATE("2023-04-01")
    WHEN data = "maio" THEN DATE("2023-05-01")
    WHEN data = "junho" THEN DATE("2023-06-01")
    WHEN data = "julho" THEN DATE("2023-07-01")
    WHEN data = "agosto" THEN DATE("2023-08-01")
    WHEN data = "setembro" THEN DATE("2023-09-01")
    WHEN data = "outubro" THEN DATE("2023-10-01")
    WHEN data = "novembro" THEN DATE("2023-11-01")
    WHEN data = "dezembro" THEN DATE("2023-12-01")
    ELSE NULL
    END data,
    SAFE_CAST(metrica_valor AS INT64) as metrica_valor,
    "numero_visitantes" as metrica_tipo,
  unnamed_0 as pais_origem,
  -- CASE
  --     WHEN unnamed_0 = 'Acre' THEN 21232
  --     WHEN unnamed_0 = 'Alagoas' THEN 20086
  --     WHEN unnamed_0 = 'Amapá' THEN 21226
  --     WHEN unnamed_0 = 'Amazonas' THEN 20087
  --     WHEN unnamed_0 = 'Bahia' THEN 20088
  --     WHEN unnamed_0 = 'Ceará' THEN 20089
  --     WHEN unnamed_0 = 'Espírito Santo' THEN 20091
  --     WHEN unnamed_0 = 'Goiás' THEN 20092
  --     WHEN unnamed_0 = 'Maranhão' THEN 20093
  --     WHEN unnamed_0 = 'Mato Grosso' THEN 20096
  --     WHEN unnamed_0 = 'Mato Grosso do Sul' THEN 20095
  --     WHEN unnamed_0 = 'Minas Gerais' THEN 20094
  --     WHEN unnamed_0 = 'Pará' THEN 20097
  --     WHEN unnamed_0 = 'Paraíba' THEN 20098
  --     WHEN unnamed_0 = 'Paraná' THEN 20101
  --     WHEN unnamed_0 = 'Pernambuco' THEN 20099
  --     WHEN unnamed_0 = 'Piauí' THEN 20100
  --     WHEN unnamed_0 = 'Rio de Janeiro' THEN 20102
  --     WHEN unnamed_0 = 'Rio Grande do Norte' THEN 20103
  --     WHEN unnamed_0 = 'Rio Grande do Sul' THEN 20104
  --     WHEN unnamed_0 = 'Rondônia' THEN 21227
  --     WHEN unnamed_0 = 'Roraima' THEN 21228
  --     WHEN unnamed_0 = 'Santa Catarina' THEN 20105
  --     WHEN unnamed_0 = 'São Paulo' THEN 20106
  --     WHEN unnamed_0 = 'Sergipe' THEN 21229
  --     WHEN unnamed_0 = 'Tocantins' THEN 21230
  --     WHEN unnamed_0 = 'Distrito Federal' THEN 20090
  --     ELSE 00000
  --   END as pais_origem_google,
  CASE 
    WHEN unnamed_0 = 'Distrito Federal' THEN "Distrito Federal, Brasil"
    ELSE unnamed_0
  END as pais_origem_google
FROM {{ ref('raw_turismo_fluxo_visitantes__claro_pais_origem_2023_clean') }}
UNPIVOT(metrica_valor FOR data IN (
    janeiro,
    fevereiro,
    marco,
    abril,
    maio,
    junho,
    julho,
    agosto,
    setembro,
    outubro,
    novembro,
    dezembro
    )
  )
),

claro_origem_pais_1 as (
  SELECT
  CASE 
    WHEN data = "janeiro" THEN DATE("2024-01-01")
    WHEN data = "fevereiro" THEN DATE("2024-02-01")
    WHEN data = "marco" THEN DATE("2024-03-01")
    WHEN data = "abril" THEN DATE("2024-04-01")
    WHEN data = "maio" THEN DATE("2024-05-01")
    WHEN data = "junho" THEN DATE("2024-06-01")
    WHEN data = "julho" THEN DATE("2024-07-01")
    WHEN data = "agosto" THEN DATE("2024-08-01")
    WHEN data = "setembro" THEN DATE("2024-09-01")
    WHEN data = "outubro" THEN DATE("2024-10-01")
    WHEN data = "novembro" THEN DATE("2024-11-01")
    WHEN data = "dezembro" THEN DATE("2024-12-01")
    ELSE NULL
    END data,
    SAFE_CAST(metrica_valor AS INT64) as metrica_valor,
    "numero_visitantes" as metrica_tipo,
  unnamed_0 as pais_origem,
  -- CASE
  --     WHEN unnamed_0 = 'Acre' THEN 21232
  --     WHEN unnamed_0 = 'Alagoas' THEN 20086
  --     WHEN unnamed_0 = 'Amapá' THEN 21226
  --     WHEN unnamed_0 = 'Amazonas' THEN 20087
  --     WHEN unnamed_0 = 'Bahia' THEN 20088
  --     WHEN unnamed_0 = 'Ceará' THEN 20089
  --     WHEN unnamed_0 = 'Espírito Santo' THEN 20091
  --     WHEN unnamed_0 = 'Goiás' THEN 20092
  --     WHEN unnamed_0 = 'Maranhão' THEN 20093
  --     WHEN unnamed_0 = 'Mato Grosso' THEN 20096
  --     WHEN unnamed_0 = 'Mato Grosso do Sul' THEN 20095
  --     WHEN unnamed_0 = 'Minas Gerais' THEN 20094
  --     WHEN unnamed_0 = 'Pará' THEN 20097
  --     WHEN unnamed_0 = 'Paraíba' THEN 20098
  --     WHEN unnamed_0 = 'Paraná' THEN 20101
  --     WHEN unnamed_0 = 'Pernambuco' THEN 20099
  --     WHEN unnamed_0 = 'Piauí' THEN 20100
  --     WHEN unnamed_0 = 'Rio de Janeiro' THEN 20102
  --     WHEN unnamed_0 = 'Rio Grande do Norte' THEN 20103
  --     WHEN unnamed_0 = 'Rio Grande do Sul' THEN 20104
  --     WHEN unnamed_0 = 'Rondônia' THEN 21227
  --     WHEN unnamed_0 = 'Roraima' THEN 21228
  --     WHEN unnamed_0 = 'Santa Catarina' THEN 20105
  --     WHEN unnamed_0 = 'São Paulo' THEN 20106
  --     WHEN unnamed_0 = 'Sergipe' THEN 21229
  --     WHEN unnamed_0 = 'Tocantins' THEN 21230
  --     WHEN unnamed_0 = 'Distrito Federal' THEN 20090
  --     ELSE 00000
  --   END as pais_origem_google,
  CASE 
    WHEN unnamed_0 = 'Distrito Federal' THEN "Distrito Federal, Brasil"
    ELSE unnamed_0
  END as pais_origem_google
FROM {{ ref('raw_turismo_fluxo_visitantes__claro_pais_origem_2024_new_clean') }}
UNPIVOT(metrica_valor FOR data IN (
    janeiro,
    fevereiro,
    marco,
    abril,
    maio,
    junho,
    julho,
    agosto,
    setembro,
    outubro,
    novembro,
    dezembro
    )
  )
),

claro_origem_pais_2 as (
  SELECT
  CASE 
    WHEN data = "janeiro" THEN DATE("2025-01-01")
    WHEN data = "fevereiro" THEN DATE("2025-02-01")
    WHEN data = "marco" THEN DATE("2025-03-01")
    WHEN data = "abril" THEN DATE("2025-04-01")
    WHEN data = "maio" THEN DATE("2025-05-01")
    WHEN data = "junho" THEN DATE("2025-06-01")
    WHEN data = "julho" THEN DATE("2025-07-01")
    WHEN data = "agosto" THEN DATE("2025-08-01")
    WHEN data = "setembro" THEN DATE("2025-09-01")
    WHEN data = "outubro" THEN DATE("2025-10-01")
    WHEN data = "novembro" THEN DATE("2025-11-01")
    WHEN data = "dezembro" THEN DATE("2025-12-01")
    ELSE NULL
    END data,
    SAFE_CAST(metrica_valor AS INT64) as metrica_valor,
    "numero_visitantes" as metrica_tipo,
  unnamed_0 as pais_origem,
  CASE 
    WHEN unnamed_0 = 'Distrito Federal' THEN "Distrito Federal, Brasil"
    ELSE unnamed_0
  END as pais_origem_google
FROM {{ ref('raw_turismo_fluxo_visitantes__claro_pais_origem_2025_clean') }}
UNPIVOT(metrica_valor FOR data IN (
    janeiro,
    fevereiro,
    marco,
    abril,
    maio,
    junho,
    julho,
    agosto,
    setembro,
    outubro,
    novembro,
    dezembro
    )
  )
),

claro_origem_pais AS (
  SELECT
    *
  FROM claro_origem_pais_0

  UNION ALL

  SELECT
    *
  FROM claro_origem_pais_1

  UNION ALL

  SELECT
    *
  FROM claro_origem_pais_2
),

compara_mes_passado AS (
  SELECT 
    *,
    CASE 
      WHEN metrica_valor IS NULL OR LAG(metrica_valor,1) OVER (PARTITION BY pais_origem ORDER BY data) IS NULL OR LAG(metrica_valor,1) OVER (PARTITION BY pais_origem ORDER BY data) = 0
      THEN NULL
      ELSE ROUND(metrica_valor/(LAG(metrica_valor,1) OVER (PARTITION BY pais_origem ORDER BY data))- 1,2)
    END as variacao_mensal 
  FROM claro_origem_pais
  WHERE data <= CURRENT_DATE()
),

data_referencia_dashboard AS ( -- a ULTIMA data em que se tem dados de visitacoes para TODOS os pontos turisticos
  WITH pt_ultima_atualizacao AS (
    SELECT
      pais_origem,
      MAX(data) data_atualizacao
    FROM compara_mes_passado 
    WHERE metrica_valor IS NOT NULL AND metrica_valor != 0
    GROUP BY pais_origem
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
WHERE pais_origem != "Sem identificação"
AND pais_origem != "TOTAL"
ORDER BY pais_origem, data