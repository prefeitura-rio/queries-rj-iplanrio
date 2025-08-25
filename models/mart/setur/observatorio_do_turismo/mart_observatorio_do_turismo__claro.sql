{{ config(
    materialized='table',
    schema='turismo_fluxo_visitantes',
    alias='claro',
) }}

WITH pontos_turisticos_geral AS (
SELECT 
  CASE 
    WHEN _2023 = "Janeiro" THEN DATE("2023-01-01")
    WHEN _2023 = "Fevereiro" THEN DATE("2023-02-01")
    WHEN _2023 = "Março" THEN DATE("2023-03-01")
    WHEN _2023 = "Abril" THEN DATE("2023-04-01")
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
  SAFE_CAST(SAFE_CAST(metrica_valor AS FLOAT64) AS INT64) as metrica_valor,
  "numero_visitantes" as metrica_tipo,
  "Geral" as origem_visitante,
  CASE
    WHEN ferramenta_turistica = "cristo_redentor" THEN "Cristo Redentor"
    WHEN ferramenta_turistica = "pao_de_acucar" THEN "Pão de Açúcar"
    WHEN ferramenta_turistica = "selaron" THEN "Escadaria Selarón"
    WHEN ferramenta_turistica = "ccbb_correios" THEN "CCBB Correios"
    WHEN ferramenta_turistica = "praia_de_copacabana_leme" THEN "Praias de Copacabana e Leme"
    WHEN ferramenta_turistica = "jardim_botanico" THEN "Jardim Botânico"
    WHEN ferramenta_turistica = "catedral_metropolitana" THEN "Catedral Metropolitana"
    WHEN ferramenta_turistica = "lapa_bairro" THEN "Lapa"
    WHEN ferramenta_turistica = "boulevard_olimpico" THEN "Boulevard Olímpico"
    WHEN ferramenta_turistica = "lagoa_rodrigo_de_freitas" THEN "Lagoa Rodrigo de Freitas"
    WHEN ferramenta_turistica = "floresta_da_tijuca" THEN "Floresta da Tijuca"
    WHEN ferramenta_turistica = "maracana" THEN "Maracanã"
    WHEN ferramenta_turistica = "pequena_africa" THEN "Centro Cultural Pequena África"    
    ELSE ferramenta_turistica 
  END ferramenta_turistica,
  ST_GEOGPOINT(`rj-setur.turismo_fluxo_visitantes`.geolocate_sight(ferramenta_turistica)[1], `rj-setur.turismo_fluxo_visitantes`.geolocate_sight(ferramenta_turistica)[0]) as ferramenta_turistica_coordenadas,
  `rj-setur.turismo_fluxo_visitantes`.geolocate_sight(ferramenta_turistica)[0] as latitude,
  `rj-setur.turismo_fluxo_visitantes`.geolocate_sight(ferramenta_turistica)[1] as longitude
FROM `rj-setur.turismo_fluxo_visitantes_staging.claro_atrativos_geral`
UNPIVOT(metrica_valor FOR ferramenta_turistica IN (
    cristo_redentor,
    pao_de_acucar,
    selaron,
    ccbb_correios,
    praia_de_copacabana_leme,
    jardim_botanico,
    catedral_metropolitana,
    lapa_bairro,
    boulevard_olimpico,
    lagoa_rodrigo_de_freitas,
    floresta_da_tijuca,
    maracana,
    pequena_africa
    )
  )

  UNION ALL

SELECT 
  CASE 
    WHEN _2023 = "Janeiro" THEN DATE("2024-01-01")
    WHEN _2023 = "Fevereiro" THEN DATE("2024-02-01")
    WHEN _2023 = "Março" THEN DATE("2024-03-01")
    WHEN _2023 = "Abril" THEN DATE("2024-04-01")
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
  SAFE_CAST(SAFE_CAST(metrica_valor AS FLOAT64) AS INT64) as metrica_valor,
  "numero_visitantes" as metrica_tipo,
  "Geral" as origem_visitante,
  CASE
    WHEN ferramenta_turistica = "cristo_redentor" THEN "Cristo Redentor"
    WHEN ferramenta_turistica = "pao_de_acucar" THEN "Pão de Açúcar"
    WHEN ferramenta_turistica = "selaron" THEN "Escadaria Selarón"
    WHEN ferramenta_turistica = "ccbb_correios" THEN "CCBB Correios"
    WHEN ferramenta_turistica = "praia_de_copacabana_leme" THEN "Praias de Copacabana e Leme"
    WHEN ferramenta_turistica = "jardim_botanico" THEN "Jardim Botânico"
    WHEN ferramenta_turistica = "catedral_metropolitana" THEN "Catedral Metropolitana"
    WHEN ferramenta_turistica = "lapa_bairro" THEN "Lapa"
    WHEN ferramenta_turistica = "boulevard_olimpico" THEN "Boulevard Olímpico"
    WHEN ferramenta_turistica = "lagoa_rodrigo_de_freitas" THEN "Lagoa Rodrigo de Freitas"
    WHEN ferramenta_turistica = "floresta_da_tijuca" THEN "Floresta da Tijuca"
    WHEN ferramenta_turistica = "maracana" THEN "Maracanã"
    WHEN ferramenta_turistica = "pequena_africa" THEN "Centro Cultural Pequena África"    
    ELSE ferramenta_turistica 
  END ferramenta_turistica,
  ST_GEOGPOINT(`rj-setur.turismo_fluxo_visitantes`.geolocate_sight(ferramenta_turistica)[1], `rj-setur.turismo_fluxo_visitantes`.geolocate_sight(ferramenta_turistica)[0]) as ferramenta_turistica_coordenadas,
  `rj-setur.turismo_fluxo_visitantes`.geolocate_sight(ferramenta_turistica)[0] as latitude,
  `rj-setur.turismo_fluxo_visitantes`.geolocate_sight(ferramenta_turistica)[1] as longitude
FROM {{ ref('raw_turismo_fluxo_visitantes__claro_atrativos_geral_2024_new_clean') }}
UNPIVOT(metrica_valor FOR ferramenta_turistica IN (
    cristo_redentor,
    pao_de_acucar,
    selaron,
    ccbb_correios,
    praia_de_copacabana_leme,
    jardim_botanico,
    catedral_metropolitana,
    lapa_bairro,
    boulevard_olimpico,
    lagoa_rodrigo_de_freitas,
    floresta_da_tijuca,
    maracana,
    pequena_africa
    )
  )

  UNION ALL

SELECT 
  CASE 
    WHEN _2023 = "Janeiro" THEN DATE("2025-01-01")
    WHEN _2023 = "Fevereiro" THEN DATE("2025-02-01")
    WHEN _2023 = "Março" THEN DATE("2025-03-01")
    WHEN _2023 = "Abril" THEN DATE("2025-04-01")
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
  SAFE_CAST(SAFE_CAST(metrica_valor AS FLOAT64) AS INT64) as metrica_valor,
  "numero_visitantes" as metrica_tipo,
  "Geral" as origem_visitante,
  CASE
    WHEN ferramenta_turistica = "cristo_redentor" THEN "Cristo Redentor"
    WHEN ferramenta_turistica = "pao_de_acucar" THEN "Pão de Açúcar"
    WHEN ferramenta_turistica = "selaron" THEN "Escadaria Selarón"
    WHEN ferramenta_turistica = "ccbb_correios" THEN "CCBB Correios"
    WHEN ferramenta_turistica = "praia_de_copacabana_leme" THEN "Praias de Copacabana e Leme"
    WHEN ferramenta_turistica = "jardim_botanico" THEN "Jardim Botânico"
    WHEN ferramenta_turistica = "catedral_metropolitana" THEN "Catedral Metropolitana"
    WHEN ferramenta_turistica = "lapa_bairro" THEN "Lapa"
    WHEN ferramenta_turistica = "boulevard_olimpico" THEN "Boulevard Olímpico"
    WHEN ferramenta_turistica = "lagoa_rodrigo_de_freitas" THEN "Lagoa Rodrigo de Freitas"
    WHEN ferramenta_turistica = "floresta_da_tijuca" THEN "Floresta da Tijuca"
    WHEN ferramenta_turistica = "maracana" THEN "Maracanã"
    WHEN ferramenta_turistica = "pequena_africa" THEN "Centro Cultural Pequena África"    
    ELSE ferramenta_turistica 
  END ferramenta_turistica,
  ST_GEOGPOINT(`rj-setur.turismo_fluxo_visitantes`.geolocate_sight(ferramenta_turistica)[1], `rj-setur.turismo_fluxo_visitantes`.geolocate_sight(ferramenta_turistica)[0]) as ferramenta_turistica_coordenadas,
  `rj-setur.turismo_fluxo_visitantes`.geolocate_sight(ferramenta_turistica)[0] as latitude,
  `rj-setur.turismo_fluxo_visitantes`.geolocate_sight(ferramenta_turistica)[1] as longitude
FROM {{ ref('raw_turismo_fluxo_visitantes__claro_atrativos_geral_2025_clean') }}
UNPIVOT(metrica_valor FOR ferramenta_turistica IN (
    cristo_redentor,
    pao_de_acucar,
    selaron,
    ccbb_correios,
    praia_de_copacabana_leme,
    jardim_botanico,
    catedral_metropolitana,
    lapa_bairro,
    boulevard_olimpico,
    lagoa_rodrigo_de_freitas,
    floresta_da_tijuca,
    maracana,
    pequena_africa
    )
  )
),

pontos_turisticos_turistas AS (
  SELECT 
  CASE 
    WHEN _2023 = "Janeiro" THEN DATE("2023-01-01")
    WHEN _2023 = "Fevereiro" THEN DATE("2023-02-01")
    WHEN _2023 = "Março" THEN DATE("2023-03-01")
    WHEN _2023 = "Abril" THEN DATE("2023-04-01")
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
  SAFE_CAST(SAFE_CAST(metrica_valor AS FLOAT64) AS INT64) as metrica_valor,
  "numero_visitantes" as metrica_tipo,
  "Turistas" as origem_visitante,
  CASE
    WHEN ferramenta_turistica = "cristo_redentor" THEN "Cristo Redentor"
    WHEN ferramenta_turistica = "pao_de_acucar" THEN "Pão de Açúcar"
    WHEN ferramenta_turistica = "selaron" THEN "Escadaria Selarón"
    WHEN ferramenta_turistica = "ccbb_correios" THEN "CCBB Correios"
    WHEN ferramenta_turistica = "praia_de_copacabana_leme" THEN "Praias de Copacabana e Leme"
    WHEN ferramenta_turistica = "jardim_botanico" THEN "Jardim Botânico"
    WHEN ferramenta_turistica = "catedral_metropolitana" THEN "Catedral Metropolitana"
    WHEN ferramenta_turistica = "lapa_bairro" THEN "Lapa"
    WHEN ferramenta_turistica = "boulevard_olimpico" THEN "Boulevard Olímpico"
    WHEN ferramenta_turistica = "lagoa_rodrigo_de_freitas" THEN "Lagoa Rodrigo de Freitas"
    WHEN ferramenta_turistica = "floresta_da_tijuca" THEN "Floresta da Tijuca"
    WHEN ferramenta_turistica = "maracana" THEN "Maracanã"
    WHEN ferramenta_turistica = "pequena_africa" THEN "Centro Cultural Pequena África"    
    ELSE ferramenta_turistica 
  END ferramenta_turistica,
  ST_GEOGPOINT(`rj-setur.turismo_fluxo_visitantes`.geolocate_sight(ferramenta_turistica)[1], `rj-setur.turismo_fluxo_visitantes`.geolocate_sight(ferramenta_turistica)[0]) as ferramenta_turistica_coordenadas,
  `rj-setur.turismo_fluxo_visitantes`.geolocate_sight(ferramenta_turistica)[0] as latitude,
  `rj-setur.turismo_fluxo_visitantes`.geolocate_sight(ferramenta_turistica)[1] as longitude
FROM `rj-setur.turismo_fluxo_visitantes_staging.claro_atrativos_turistas`
UNPIVOT(metrica_valor FOR ferramenta_turistica IN (
    cristo_redentor,
    pao_de_acucar,
    selaron,
    ccbb_correios,
    praia_de_copacabana_leme,
    jardim_botanico,
    catedral_metropolitana,
    lapa_bairro,
    boulevard_olimpico,
    lagoa_rodrigo_de_freitas,
    floresta_da_tijuca,
    maracana,
    pequena_africa
    )
  )

  UNION ALL

SELECT 
  CASE 
    WHEN _2023 = "Janeiro" THEN DATE("2024-01-01")
    WHEN _2023 = "Fevereiro" THEN DATE("2024-02-01")
    WHEN _2023 = "Março" THEN DATE("2024-03-01")
    WHEN _2023 = "Abril" THEN DATE("2024-04-01")
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
  SAFE_CAST(SAFE_CAST(metrica_valor AS FLOAT64) AS INT64) as metrica_valor,
  "numero_visitantes" as metrica_tipo,
  "Turistas" as origem_visitante,
  CASE
    WHEN ferramenta_turistica = "cristo_redentor" THEN "Cristo Redentor"
    WHEN ferramenta_turistica = "pao_de_acucar" THEN "Pão de Açúcar"
    WHEN ferramenta_turistica = "selaron" THEN "Escadaria Selarón"
    WHEN ferramenta_turistica = "ccbb_correios" THEN "CCBB Correios"
    WHEN ferramenta_turistica = "praia_de_copacabana_leme" THEN "Praias de Copacabana e Leme"
    WHEN ferramenta_turistica = "jardim_botanico" THEN "Jardim Botânico"
    WHEN ferramenta_turistica = "catedral_metropolitana" THEN "Catedral Metropolitana"
    WHEN ferramenta_turistica = "lapa_bairro" THEN "Lapa"
    WHEN ferramenta_turistica = "boulevard_olimpico" THEN "Boulevard Olímpico"
    WHEN ferramenta_turistica = "lagoa_rodrigo_de_freitas" THEN "Lagoa Rodrigo de Freitas"
    WHEN ferramenta_turistica = "floresta_da_tijuca" THEN "Floresta da Tijuca"
    WHEN ferramenta_turistica = "maracana" THEN "Maracanã"
    WHEN ferramenta_turistica = "pequena_africa" THEN "Centro Cultural Pequena África"    
    ELSE ferramenta_turistica 
  END ferramenta_turistica,
  ST_GEOGPOINT(`rj-setur.turismo_fluxo_visitantes`.geolocate_sight(ferramenta_turistica)[1], `rj-setur.turismo_fluxo_visitantes`.geolocate_sight(ferramenta_turistica)[0]) as ferramenta_turistica_coordenadas,
  `rj-setur.turismo_fluxo_visitantes`.geolocate_sight(ferramenta_turistica)[0] as latitude,
  `rj-setur.turismo_fluxo_visitantes`.geolocate_sight(ferramenta_turistica)[1] as longitude
FROM {{ ref('raw_turismo_fluxo_visitantes__claro_atrativos_turistas_2024_new_clean') }}
UNPIVOT(metrica_valor FOR ferramenta_turistica IN (
    cristo_redentor,
    pao_de_acucar,
    selaron,
    ccbb_correios,
    praia_de_copacabana_leme,
    jardim_botanico,
    catedral_metropolitana,
    lapa_bairro,
    boulevard_olimpico,
    lagoa_rodrigo_de_freitas,
    floresta_da_tijuca,
    maracana,
    pequena_africa
    )
  )

  UNION ALL

SELECT 
  CASE 
    WHEN _2023 = "Janeiro" THEN DATE("2025-01-01")
    WHEN _2023 = "Fevereiro" THEN DATE("2025-02-01")
    WHEN _2023 = "Março" THEN DATE("2025-03-01")
    WHEN _2023 = "Abril" THEN DATE("2025-04-01")
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
  SAFE_CAST(SAFE_CAST(metrica_valor AS FLOAT64) AS INT64) as metrica_valor,
  "numero_visitantes" as metrica_tipo,
  "Turistas" as origem_visitante,
  CASE
    WHEN ferramenta_turistica = "cristo_redentor" THEN "Cristo Redentor"
    WHEN ferramenta_turistica = "pao_de_acucar" THEN "Pão de Açúcar"
    WHEN ferramenta_turistica = "selaron" THEN "Escadaria Selarón"
    WHEN ferramenta_turistica = "ccbb_correios" THEN "CCBB Correios"
    WHEN ferramenta_turistica = "praia_de_copacabana_leme" THEN "Praias de Copacabana e Leme"
    WHEN ferramenta_turistica = "jardim_botanico" THEN "Jardim Botânico"
    WHEN ferramenta_turistica = "catedral_metropolitana" THEN "Catedral Metropolitana"
    WHEN ferramenta_turistica = "lapa_bairro" THEN "Lapa"
    WHEN ferramenta_turistica = "boulevard_olimpico" THEN "Boulevard Olímpico"
    WHEN ferramenta_turistica = "lagoa_rodrigo_de_freitas" THEN "Lagoa Rodrigo de Freitas"
    WHEN ferramenta_turistica = "floresta_da_tijuca" THEN "Floresta da Tijuca"
    WHEN ferramenta_turistica = "maracana" THEN "Maracanã"
    WHEN ferramenta_turistica = "pequena_africa" THEN "Centro Cultural Pequena África"    
    ELSE ferramenta_turistica 
  END ferramenta_turistica,
  ST_GEOGPOINT(`rj-setur.turismo_fluxo_visitantes`.geolocate_sight(ferramenta_turistica)[1], `rj-setur.turismo_fluxo_visitantes`.geolocate_sight(ferramenta_turistica)[0]) as ferramenta_turistica_coordenadas,
  `rj-setur.turismo_fluxo_visitantes`.geolocate_sight(ferramenta_turistica)[0] as latitude,
  `rj-setur.turismo_fluxo_visitantes`.geolocate_sight(ferramenta_turistica)[1] as longitude
FROM {{ ref('raw_turismo_fluxo_visitantes__claro_atrativos_turistas_2025_clean') }}
UNPIVOT(metrica_valor FOR ferramenta_turistica IN (
    cristo_redentor,
    pao_de_acucar,
    selaron,
    ccbb_correios,
    praia_de_copacabana_leme,
    jardim_botanico,
    catedral_metropolitana,
    lapa_bairro,
    boulevard_olimpico,
    lagoa_rodrigo_de_freitas,
    floresta_da_tijuca,
    maracana,
    pequena_africa
    )
  )
),

pontos_turisticos as (
  SELECT
    ptg.data,
    (ptg.metrica_valor - ptt.metrica_valor) as metrica_valor,
    ptg.metrica_tipo,
    "Cariocas" as origem_visitante,
    ptg.ferramenta_turistica,
    ptg.ferramenta_turistica_coordenadas,
    ptg.latitude,
    ptg.longitude
  FROM pontos_turisticos_geral AS ptg
  LEFT JOIN pontos_turisticos_turistas AS ptt
  ON ptg.data = ptt.data AND ptg.ferramenta_turistica = ptt.ferramenta_turistica

  UNION ALL

  SELECT
    *
  FROM pontos_turisticos_turistas
),
  
compara_mes_passado AS (
  SELECT 
    *,
    CASE 
      WHEN metrica_valor IS NULL OR LAG(metrica_valor,1) OVER (PARTITION BY ferramenta_turistica, origem_visitante ORDER BY data) IS NULL OR LAG(metrica_valor,1) OVER (PARTITION BY ferramenta_turistica, origem_visitante ORDER BY data) = 0
      THEN NULL
      ELSE ROUND(metrica_valor/(LAG(metrica_valor,1) OVER (PARTITION BY ferramenta_turistica, origem_visitante ORDER BY data))- 1,2)
    END as variacao_mensal 
  FROM pontos_turisticos
  WHERE data <= CURRENT_DATE()
),

data_referencia_dashboard AS ( -- a ULTIMA data em que se tem dados de visitacoes para TODOS os pontos turisticos
  WITH pt_ultima_atualizacao AS (
    SELECT
      ferramenta_turistica,
      origem_visitante,
      MAX(data) data_atualizacao
    FROM compara_mes_passado 
    WHERE metrica_valor IS NOT NULL AND metrica_valor != 0
    GROUP BY ferramenta_turistica, origem_visitante
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
ORDER BY ferramenta_turistica, data