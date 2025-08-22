{{ config(
    materialized='table',
    schema='turismo_fluxo_visitantes',
    alias='claro_origem',
) }}

WITH claro_origem_estado_0 as (
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
    SAFE_CAST(SAFE_CAST(REPLACE(REPLACE(metrica_valor, ".", ""),",", ".") AS FLOAT64) AS INT64) as metrica_valor,
    "numero_visitantes" as metrica_tipo,
  unnamed_0 as estado_origem,
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
  --   END as estado_origem_google,
  CASE 
    WHEN unnamed_0 = 'Distrito Federal' THEN "Distrito Federal, Brasil"
    ELSE unnamed_0
  END as estado_origem_google,
    CASE
      WHEN unnamed_0 = 'Acre' THEN 'AC'
      WHEN unnamed_0 = 'Alagoas' THEN 'AL'
      WHEN unnamed_0 = 'Amapá' THEN 'AP'
      WHEN unnamed_0 = 'Amazonas' THEN 'AM'
      WHEN unnamed_0 = 'Bahia' THEN 'BA'
      WHEN unnamed_0 = 'Ceará' THEN 'CE'
      WHEN unnamed_0 = 'Espírito Santo' THEN 'ES'
      WHEN unnamed_0 = 'Goiás' THEN 'GO'
      WHEN unnamed_0 = 'Maranhão' THEN 'MA'
      WHEN unnamed_0 = 'Mato Grosso' THEN 'MT'
      WHEN unnamed_0 = 'Mato Grosso do Sul' THEN 'MS'
      WHEN unnamed_0 = 'Minas Gerais' THEN 'MG'
      WHEN unnamed_0 = 'Pará' THEN 'PA'
      WHEN unnamed_0 = 'Paraíba' THEN 'PB'
      WHEN unnamed_0 = 'Paraná' THEN 'PR'
      WHEN unnamed_0 = 'Pernambuco' THEN 'PE'
      WHEN unnamed_0 = 'Piauí' THEN 'PI'
      WHEN unnamed_0 = 'Rio de Janeiro' THEN 'RJ'
      WHEN unnamed_0 = 'Rio Grande do Norte' THEN 'RN'
      WHEN unnamed_0 = 'Rio Grande do Sul' THEN 'RS'
      WHEN unnamed_0 = 'Rondônia' THEN 'RO'
      WHEN unnamed_0 = 'Roraima' THEN 'RR'
      WHEN unnamed_0 = 'Santa Catarina' THEN 'SC'
      WHEN unnamed_0 = 'São Paulo' THEN 'SP'
      WHEN unnamed_0 = 'Sergipe' THEN 'SE'
      WHEN unnamed_0 = 'Tocantins' THEN 'TO'
      WHEN unnamed_0 = 'Distrito Federal' THEN 'DF'
      ELSE '000'
    END estado_origem_sigla,
    CASE 
      WHEN unnamed_0 IN ('Goiás', 'Mato Grosso', 'Mato Grosso do Sul', 'Distrito Federal') THEN 'Centro Oeste'
      WHEN unnamed_0 IN ('Alagoas', 'Bahia', 'Ceará', 'Maranhão', 'Paraíba', 'Pernambuco', 'Piauí', 'Rio Grande do Norte', 'Sergipe') THEN 'Nordeste'
      WHEN unnamed_0 IN ('Acre', 'Amapá', 'Amazonas', 'Pará', 'Rondônia', 'Roraima', 'Tocantins') THEN 'Norte'
      WHEN unnamed_0 IN ('Espírito Santo', 'Minas Gerais', 'Rio de Janeiro', 'São Paulo') THEN 'Sudeste'
      WHEN unnamed_0 IN ('Paraná', 'Rio Grande do Sul', 'Santa Catarina') THEN 'Sul'
      ELSE '000'
    END regiao
FROM {{ ref('raw_turismo_fluxo_visitantes__claro_estado_origem_2023_clean') }}
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

claro_origem_estado_1 as (
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
    SAFE_CAST(SAFE_CAST(REPLACE(REPLACE(metrica_valor, ".", ""),",", ".") AS FLOAT64) AS INT64) as metrica_valor,
    "numero_visitantes" as metrica_tipo,
  unnamed_0 as estado_origem,
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
  --   END as estado_origem_google,
  CASE 
    WHEN unnamed_0 = 'Distrito Federal' THEN "Distrito Federal, Brasil"
    ELSE unnamed_0
  END as estado_origem_google,
    CASE
      WHEN unnamed_0 = 'Acre' THEN 'AC'
      WHEN unnamed_0 = 'Alagoas' THEN 'AL'
      WHEN unnamed_0 = 'Amapá' THEN 'AP'
      WHEN unnamed_0 = 'Amazonas' THEN 'AM'
      WHEN unnamed_0 = 'Bahia' THEN 'BA'
      WHEN unnamed_0 = 'Ceará' THEN 'CE'
      WHEN unnamed_0 = 'Espírito Santo' THEN 'ES'
      WHEN unnamed_0 = 'Goiás' THEN 'GO'
      WHEN unnamed_0 = 'Maranhão' THEN 'MA'
      WHEN unnamed_0 = 'Mato Grosso' THEN 'MT'
      WHEN unnamed_0 = 'Mato Grosso do Sul' THEN 'MS'
      WHEN unnamed_0 = 'Minas Gerais' THEN 'MG'
      WHEN unnamed_0 = 'Pará' THEN 'PA'
      WHEN unnamed_0 = 'Paraíba' THEN 'PB'
      WHEN unnamed_0 = 'Paraná' THEN 'PR'
      WHEN unnamed_0 = 'Pernambuco' THEN 'PE'
      WHEN unnamed_0 = 'Piauí' THEN 'PI'
      WHEN unnamed_0 = 'Rio de Janeiro' THEN 'RJ'
      WHEN unnamed_0 = 'Rio Grande do Norte' THEN 'RN'
      WHEN unnamed_0 = 'Rio Grande do Sul' THEN 'RS'
      WHEN unnamed_0 = 'Rondônia' THEN 'RO'
      WHEN unnamed_0 = 'Roraima' THEN 'RR'
      WHEN unnamed_0 = 'Santa Catarina' THEN 'SC'
      WHEN unnamed_0 = 'São Paulo' THEN 'SP'
      WHEN unnamed_0 = 'Sergipe' THEN 'SE'
      WHEN unnamed_0 = 'Tocantins' THEN 'TO'
      WHEN unnamed_0 = 'Distrito Federal' THEN 'DF'
      ELSE '000'
    END estado_origem_sigla,
    CASE 
      WHEN unnamed_0 IN ('Goiás', 'Mato Grosso', 'Mato Grosso do Sul', 'Distrito Federal') THEN 'Centro Oeste'
      WHEN unnamed_0 IN ('Alagoas', 'Bahia', 'Ceará', 'Maranhão', 'Paraíba', 'Pernambuco', 'Piauí', 'Rio Grande do Norte', 'Sergipe') THEN 'Nordeste'
      WHEN unnamed_0 IN ('Acre', 'Amapá', 'Amazonas', 'Pará', 'Rondônia', 'Roraima', 'Tocantins') THEN 'Norte'
      WHEN unnamed_0 IN ('Espírito Santo', 'Minas Gerais', 'Rio de Janeiro', 'São Paulo') THEN 'Sudeste'
      WHEN unnamed_0 IN ('Paraná', 'Rio Grande do Sul', 'Santa Catarina') THEN 'Sul'
      ELSE '000'
    END regiao
FROM {{ ref('raw_turismo_fluxo_visitantes__claro_estado_origem_2024_clean') }}
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

claro_origem_estado_2 as (
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
    SAFE_CAST(SAFE_CAST(REPLACE(REPLACE(metrica_valor, ".", ""),",", ".") AS FLOAT64) AS INT64) as metrica_valor,
    "numero_visitantes" as metrica_tipo,
  unnamed_0 as estado_origem,
  CASE 
    WHEN unnamed_0 = 'Distrito Federal' THEN "Distrito Federal, Brasil"
    ELSE unnamed_0
  END as estado_origem_google,
    CASE
      WHEN unnamed_0 = 'Acre' THEN 'AC'
      WHEN unnamed_0 = 'Alagoas' THEN 'AL'
      WHEN unnamed_0 = 'Amapá' THEN 'AP'
      WHEN unnamed_0 = 'Amazonas' THEN 'AM'
      WHEN unnamed_0 = 'Bahia' THEN 'BA'
      WHEN unnamed_0 = 'Ceará' THEN 'CE'
      WHEN unnamed_0 = 'Espírito Santo' THEN 'ES'
      WHEN unnamed_0 = 'Goiás' THEN 'GO'
      WHEN unnamed_0 = 'Maranhão' THEN 'MA'
      WHEN unnamed_0 = 'Mato Grosso' THEN 'MT'
      WHEN unnamed_0 = 'Mato Grosso do Sul' THEN 'MS'
      WHEN unnamed_0 = 'Minas Gerais' THEN 'MG'
      WHEN unnamed_0 = 'Pará' THEN 'PA'
      WHEN unnamed_0 = 'Paraíba' THEN 'PB'
      WHEN unnamed_0 = 'Paraná' THEN 'PR'
      WHEN unnamed_0 = 'Pernambuco' THEN 'PE'
      WHEN unnamed_0 = 'Piauí' THEN 'PI'
      WHEN unnamed_0 = 'Rio de Janeiro' THEN 'RJ'
      WHEN unnamed_0 = 'Rio Grande do Norte' THEN 'RN'
      WHEN unnamed_0 = 'Rio Grande do Sul' THEN 'RS'
      WHEN unnamed_0 = 'Rondônia' THEN 'RO'
      WHEN unnamed_0 = 'Roraima' THEN 'RR'
      WHEN unnamed_0 = 'Santa Catarina' THEN 'SC'
      WHEN unnamed_0 = 'São Paulo' THEN 'SP'
      WHEN unnamed_0 = 'Sergipe' THEN 'SE'
      WHEN unnamed_0 = 'Tocantins' THEN 'TO'
      WHEN unnamed_0 = 'Distrito Federal' THEN 'DF'
      ELSE '000'
    END estado_origem_sigla,
    CASE 
      WHEN unnamed_0 IN ('Goiás', 'Mato Grosso', 'Mato Grosso do Sul', 'Distrito Federal') THEN 'Centro Oeste'
      WHEN unnamed_0 IN ('Alagoas', 'Bahia', 'Ceará', 'Maranhão', 'Paraíba', 'Pernambuco', 'Piauí', 'Rio Grande do Norte', 'Sergipe') THEN 'Nordeste'
      WHEN unnamed_0 IN ('Acre', 'Amapá', 'Amazonas', 'Pará', 'Rondônia', 'Roraima', 'Tocantins') THEN 'Norte'
      WHEN unnamed_0 IN ('Espírito Santo', 'Minas Gerais', 'Rio de Janeiro', 'São Paulo') THEN 'Sudeste'
      WHEN unnamed_0 IN ('Paraná', 'Rio Grande do Sul', 'Santa Catarina') THEN 'Sul'
      ELSE '000'
    END regiao
FROM {{ ref('raw_turismo_fluxo_visitantes__claro_estado_origem_2025_clean') }}
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

claro_origem_estado AS (
  SELECT
    coe.*,
    ST_GEOGFROMTEXT(e.geometry) as estado_geometry
    FROM claro_origem_estado_0 as coe
  LEFT JOIN `rj-setur.turismo_fluxo_visitantes_staging.brasil_estados` as e
    ON coe.estado_origem_sigla = e.abbrev_state
  
  UNION ALL

  SELECT
    coe.*,
    ST_GEOGFROMTEXT(e.geometry) as estado_geometry
    FROM claro_origem_estado_1 as coe
  LEFT JOIN `rj-setur.turismo_fluxo_visitantes_staging.brasil_estados` as e
    ON coe.estado_origem_sigla = e.abbrev_state

  UNION ALL

  SELECT
    coe.*,
    ST_GEOGFROMTEXT(e.geometry) as estado_geometry
    FROM claro_origem_estado_2 as coe
  LEFT JOIN `rj-setur.turismo_fluxo_visitantes_staging.brasil_estados` as e
    ON coe.estado_origem_sigla = e.abbrev_state
),

nacionalidades AS (
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

  UNION ALL

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

  UNION ALL

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

claro_origem AS (
SELECT
  data,
  metrica_valor,
  metrica_tipo,
  "Internacional" as estado_origem,
  SAFE_CAST(NULL AS STRING) AS estado_origem_google,
  SAFE_CAST(NULL AS STRING) AS estado_origem_sigla,
  SAFE_CAST(NULL AS STRING) as regiao,
  NULL as estado_geometry,
  TRUE as indicador_internacional
FROM nacionalidades
WHERE origem = "internacionais"

UNION ALL

SELECT *, FALSE as indicador_internacional FROM claro_origem_estado
),


compara_mes_passado AS (
  SELECT 
    *,
    CASE 
      WHEN metrica_valor IS NULL OR LAG(metrica_valor,1) OVER (PARTITION BY estado_origem ORDER BY data) IS NULL OR LAG(metrica_valor,1) OVER (PARTITION BY estado_origem ORDER BY data) = 0
      THEN NULL
      ELSE ROUND(metrica_valor/(LAG(metrica_valor,1) OVER (PARTITION BY estado_origem ORDER BY data))- 1,2)
    END as variacao_mensal 
  FROM claro_origem
  WHERE data <= CURRENT_DATE()
),

data_referencia_dashboard AS ( -- a ULTIMA data em que se tem dados de visitacoes para TODOS os pontos turisticos
  WITH pt_ultima_atualizacao AS (
    SELECT
      estado_origem,
      MAX(data) data_atualizacao
    FROM compara_mes_passado 
    WHERE metrica_valor IS NOT NULL AND metrica_valor != 0
    GROUP BY estado_origem
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
WHERE estado_origem != "Sem identificação"
ORDER BY estado_origem, data