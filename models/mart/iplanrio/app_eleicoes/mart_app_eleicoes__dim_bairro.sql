{{
    config(
        schema="app_eleicoes",
        alias="dim_bairro",
        materialized="table",
        tags=["app_eleicoes"],
    )
}}

-- bairros do município do Rio de Janeiro (dados mestres da prefeitura)
select
    id_bairro,
    '3304557' as id_municipio,
    nome,
    subprefeitura,
    id_area_planejamento,
    id_regiao_planejamento,
    nome_regiao_planejamento,
    id_regiao_administrativa,
    nome_regiao_administrativa,
    st_area(geometry) / 1000000 as area_km2,
    st_y(st_centroid(geometry)) as latitude,
    st_x(st_centroid(geometry)) as longitude,
    geometry
from {{ ref("raw_datario_dados_mestres__bairro") }}
