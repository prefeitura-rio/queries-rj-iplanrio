{# DECLARE step FLOAT64 DEFAULT 0.0025;  -- latitude step
-- Boounding box deve ser divisivel pelo step para garantir o snap no grid do Plus Code.
DECLARE bbox ARRAY<FLOAT64> DEFAULT [-43.8, -23.1, -43.0, -22.7];  -- [W,S,E,N]

WITH
    -- Todos os seus CTEs permanecem exatamente os mesmos.
    lats AS (
        SELECT latitude
        FROM UNNEST(GENERATE_ARRAY(bbox[OFFSET(1)], bbox[OFFSET(3)], step)) AS latitude
    ),
    lons AS (
        SELECT longitude
        FROM UNNEST(GENERATE_ARRAY(bbox[OFFSET(0)], bbox[OFFSET(2)], step)) AS longitude
    ),
    grid_centers AS (
        SELECT
            latitude + (step / 2) AS center_lat,
            longitude + (step / 2) AS center_lon
        FROM lats
        CROSS JOIN lons
    ),
    rio_boundary AS (
        SELECT geometria
        FROM `basedosdados`.`br_geobr_mapas`.`municipio`
        WHERE id_municipio = '3304557'
    ),
    inside AS (
        SELECT center_lat, center_lon
        FROM grid_centers, rio_boundary
        WHERE ST_CONTAINS(ST_BUFFER(geometria, 100), ST_GEOGPOINT(center_lon, center_lat))
    ),
    final_tb AS (
        SELECT
            tools.encode_pluscode(center_lat, center_lon, 8) AS plus8,
            tools.encode_pluscode(center_lat, center_lon, 6) AS plus6,
            ANY_VALUE(ST_GEOGPOINT(center_lon, center_lat)) AS geometry,
            ANY_VALUE(ST_GEOGFROMTEXT(
                FORMAT('LINESTRING(%f %f, %f %f, %f %f, %f %f, %f %f)',
                    center_lon - (step / 2), center_lat - (step / 2),  -- Canto Sudoeste (SW)
                    center_lon + (step / 2), center_lat - (step / 2),  -- Canto Sudeste (SE)
                    center_lon + (step / 2), center_lat + (step / 2),  -- Canto Nordeste (NE)
                    center_lon - (step / 2), center_lat + (step / 2),  -- Canto Noroeste (NW)
                    center_lon - (step / 2), center_lat - (step / 2)   -- Volta ao SW para fechar
                )
            )) AS grid_boundary
        FROM inside
        GROUP BY 1, 2
    ),
    grid as (
      SELECT
        plus8,
        plus6,
        ST_ASTEXT(geometry) as centro_geometry
      FROM final_tb
    ),
    equipamentos_territorio AS (
      SELECT
        e.secretaria_responsavel,
        e.categoria,
        e.id_equipamento,
        ST_ASTEXT(e.geometry) as geometry
      FROM `rj-iplanrio.plus_codes.equipamentos` e
      WHERE fonte in ('`rj-iplanrio`.`brutos_equipamentos`.`saude_unidades`')
    ),
    tb AS (
      SELECT
        g.plus8,
        g.centro_geometry,
        e.id_equipamento,
        e.secretaria_responsavel,
        e.categoria,
        e.geometry
      FROM grid g
      LEFT JOIN equipamentos_territorio e
        ON ST_CONTAINS(ST_GEOGFROMTEXT(e.geometry), ST_GEOGFROMTEXT(g.centro_geometry))
    )

SELECT DISTINCT
  g.plus8,
  e.id_equipamento,
  e.geometry,
FROM equipamentos_territorio AS e
LEFT JOIN tb AS g
  ON e.geometry = g.geometry
WHERE plus8 IS NULL #}