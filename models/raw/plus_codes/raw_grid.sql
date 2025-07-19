{# {{
    config(
        alias="codes",
        schema="plus_codes",
        materialized="table",
    )
}}

-- Passo 1: Definir o passo com base no tamanho de uma célula de Plus Code de 8 dígitos.
-- https://github.com/google/open-location-code/blob/main/Documentation/Specification/specification.md#code-precision
DECLARE step FLOAT64 DEFAULT 0.0025;  -- latitude step
-- Boounding box deve ser divisivel pelo step para garantir o snap no grid do Plus Code.
DECLARE bbox ARRAY<FLOAT64> DEFAULT [-43.8, -23.1, -43.0, -22.7];  -- [W,S,E,N]


CREATE OR REPLACE TABLE `rj-iplanrio.plus_codes.grid` AS (


{# CREATE OR REPLACE FUNCTION tools.encode_pluscode(latitude FLOAT64, longitude FLOAT64, len INT64)
RETURNS STRING
LANGUAGE js
OPTIONS(library=['gs://rj-escritorio-dev/raw/js_packages/openlocationcode.min.js']) AS '''
  return OpenLocationCode.encode(latitude, longitude, len);
'''; #}
WITH
    -- Gera a grade de latitudes alinhada com o grid do Plus Code.
    lats AS (
        SELECT latitude
        FROM UNNEST(GENERATE_ARRAY(bbox[OFFSET(1)], bbox[OFFSET(3)], step)) AS latitude
    ),
    -- Gera a grade de longitudes alinhada com o grid do Plus Code.
    lons AS (
        SELECT longitude
        FROM UNNEST(GENERATE_ARRAY(bbox[OFFSET(0)], bbox[OFFSET(2)], step)) AS longitude
    ),
    -- Cria os pontos da grade e calcula o centro de cada célula.
    grid_centers AS (
        SELECT
            latitude + (step / 2) AS center_lat,
            longitude + (step / 2) AS center_lon
        FROM lats
        CROSS JOIN lons
    ),
    -- Obtém a geometria do município do Rio de Janeiro.
    rio_boundary AS (
        SELECT geometria
        FROM {{ source('br_geobr_mapas', 'municipio') }}
        WHERE id_municipio = '3304557'
    ),
    -- Filtra a grade para manter apenas os pontos que estão dentro do município.
    inside AS (
        SELECT center_lat, center_lon
        FROM grid_centers, rio_boundary
        -- 100 metros de buffer para garantir que o ponto está dentro do município.
        WHERE ST_CONTAINS(ST_BUFFER(geometria, 100), ST_GEOGPOINT(center_lon, center_lat))
    ),
    -- Agrupa os resultados e gera os Plus Codes.
    final_tb AS (
        SELECT
            tools.encode_pluscode(center_lat, center_lon, 8) AS plus8,
            tools.encode_pluscode(center_lat, center_lon, 6) AS plus6,
            -- any_value é usado aqui pois já estamos agrupando por plus8 e plus6
            ANY_VALUE(ST_GEOGPOINT(center_lon, center_lat)) AS geometry
        FROM inside
        GROUP BY 1, 2
    )

-- Seleciona o resultado final.
SELECT
    plus8,
    plus6,
    geometry
FROM final_tb
)
 #}
