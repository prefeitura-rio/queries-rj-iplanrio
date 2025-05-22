{{
    config(
        alias="codes",
        schema="plus_codes",
        materialized="table",
    )
}}

DECLARE step FLOAT64 DEFAULT 0.0001;  -- latitude step
DECLARE bbox ARRAY<FLOAT64> DEFAULT [-43.8, -23.1, -43.0, -22.7];  -- [W,S,E,N]


CREATE OR REPLACE TABLE `rj-iplanrio.plus_codes.grid` AS (


{# CREATE OR REPLACE FUNCTION tools.encode_pluscode(latitude FLOAT64, longitude FLOAT64, len INT64)
RETURNS STRING
LANGUAGE js
OPTIONS(library=['gs://rj-escritorio-dev/raw/js_packages/openlocationcode.min.js']) AS '''
  return OpenLocationCode.encode(latitude, longitude, len);
'''; #}
with
    lats as (
        select latitude
        from unnest(generate_array(bbox[offset(1)], bbox[offset(3)], step)) as latitude
    ),
    lons as (
        select longitude
        from unnest(generate_array(bbox[offset(0)], bbox[offset(2)], step)) as longitude
    ),
    grid as (select latitude, longitude from lats cross join lons),
    rio_boundary as (
        select geometria
        from `basedosdados.br_geobr_mapas.municipio`
        where id_municipio = '3304557'
    ),
    inside as (
        select latitude, longitude
        from grid, rio_boundary
        where st_contains(geometria, st_geogpoint(longitude, latitude))
    ),
    final_tb as (
        select
            tools.encode_pluscode(latitude, longitude, 8) as plus8,
            tools.encode_pluscode(latitude, longitude, 6) as plus6,
            any_value(st_geogpoint(longitude, latitude)) geometry,
        from inside
        group by 1, 2
    )

select 
    plus8,
    plus6,
    geometry
from final_tb
)

