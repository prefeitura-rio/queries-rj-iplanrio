{{
    config(
        schema="app_eleicoes",
        alias="dim_municipio",
        materialized="table",
        tags=["app_eleicoes"],
    )
}}

-- municípios do RJ, com os detalhes do diretório da Base dos Dados e o limite do IBGE
select
    m.id_municipio,
    m.id_municipio_6,
    m.id_municipio_tse,
    m.nome,
    m.sigla_uf,
    m.capital_uf = 1 as indicador_capital,
    m.ddd,
    m.nome_regiao_metropolitana,
    m.nome_regiao_imediata,
    m.nome_regiao_intermediaria,
    m.nome_microrregiao,
    m.nome_mesorregiao,
    st_y(m.centroide) as latitude,
    st_x(m.centroide) as longitude,
    st_area(g.geometria) / 1000000 as area_km2,
    g.geometria as geometry
from {{ source("basedosdados_br_bd_diretorios_brasil", "municipio") }} as m
left join
    {{ source("basedosdados_br_geobr_mapas", "municipio") }} as g
    on m.id_municipio = g.id_municipio
where m.sigla_uf = 'RJ'
