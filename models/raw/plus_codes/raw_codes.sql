
{{
    config(
        alias="codes",
        schema="plus_codes",
        materialized="table",
    )
}}

DECLARE raio_padrao INT64 DEFAULT 100000;  -- metros; ajuste se precisar

{# CREATE OR REPLACE TABLE `rj-iplanrio.plus_codes.codes` AS ( #}

WITH
grid AS (
  SELECT
    plus8,
    geometry AS centro_geometry
  FROM {{ source('plus_codes', 'grid') }}
),

-- 2) Pares grid × equipamento dentro do raio
pairs AS (
  SELECT
    g.plus8,
    g.centro_geometry,
    e.tipo_equipamento AS categoria,

    -- Struct contendo TODA a linha do equipamento + distância
    (SELECT AS STRUCT
        e.*,                                           -- todos os campos de equipamentos_geo
        ST_DISTANCE(e.geometry, g.centro_geometry) AS distancia_metros
    ) AS equip_full
  FROM grid AS g
  JOIN {{ ref("raw_equipamentos")}} AS e
    ON ST_DWITHIN(e.geometry, g.centro_geometry, raio_padrao)
),

-- 3) Rankeia por distância
ranqueado AS (
  SELECT
    plus8,
    categoria,
    equip_full.distancia_metros AS distancia_metros,
    centro_geometry,
    equip_full,
    ROW_NUMBER() OVER (
      PARTITION BY plus8, categoria
      ORDER BY equip_full.distancia_metros
    ) AS rn
  FROM pairs
)

-- 4) Agrega os 3 mais próximos
SELECT
  plus8,
  categoria,
  ANY_VALUE(centro_geometry) AS geometry,
  ARRAY_AGG(equip_full ORDER BY distancia_metros LIMIT 3) AS equipamentos,
  CURRENT_TIMESTAMP()    AS ingestion_timestamp
FROM ranqueado
WHERE rn <= 3
GROUP BY plus8, categoria
{# ) #}