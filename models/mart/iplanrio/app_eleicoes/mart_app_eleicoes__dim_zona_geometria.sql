{{
    config(
        schema="app_eleicoes",
        alias="dim_zona_geometria",
        materialized="table",
        tags=["app_eleicoes"],
    )
}}

-- polígono APROXIMADO de cada zona. não existe fonte oficial de limites de zona, então:
-- 1) cada setor censitário recebe a zona do local de votação mais próximo (pontos de 2024)
-- 2) os setores são unidos por zona e simplificados
with
    locais as (
        select l.id_zona, z.id_municipio, l.geometry
        from {{ ref("mart_app_eleicoes__dim_local_votacao") }} as l
        inner join {{ ref("mart_app_eleicoes__dim_zona") }} as z on l.id_zona = z.id_zona
        where l.ano = 2024 and l.geometry is not null
    ),

    setores as (
        select
            id_municipio,
            id_setor_censitario,
            geometria,
            st_centroid(geometria) as centroide
        from {{ source("basedosdados_br_geobr_mapas", "setor_censitario_2010") }}
        where sigla_uf = 'RJ'
    ),

    -- ordena os locais do mesmo município pela distância ao centroide do setor
    distancias as (
        select
            s.id_setor_censitario,
            s.geometria,
            l.id_zona,
            row_number() over (
                partition by s.id_setor_censitario
                order by st_distance(s.centroide, l.geometry)
            ) as posicao
        from setores as s
        inner join locais as l on s.id_municipio = l.id_municipio
    ),

    zonas as (
        select id_zona, st_union_agg(geometria) as geometria
        from distancias
        where posicao = 1
        group by id_zona
    ),

    simplificadas as (
        -- tolerância de 20 metros para reduzir o peso do payload do mapa
        select id_zona, st_simplify(geometria, 20) as geometry from zonas
    )

select
    id_zona,
    geometry,
    st_astext(geometry) as geometria_wkt,
    round(st_area(geometry) / 1000000, 3) as area_km2,
    'setor_2010_local_mais_proximo' as metodo,
    'br_geobr_mapas.setor_censitario_2010 e perfil_eleitorado_local_votacao (2024)' as fonte,
    false as indicador_oficial,
    2024 as ano_referencia
from simplificadas
