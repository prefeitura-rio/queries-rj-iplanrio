{{
    config(
        schema="app_eleicoes",
        alias="dim_secao",
        materialized="table",
        cluster_by=["ano", "id_zona"],
        tags=["app_eleicoes"],
    )
}}

-- seções do RJ por ano, com coordenadas. o TSE não publicou lat/lng de 2026, então as
-- coordenadas ausentes são herdadas de 2024 (mesma seção, depois mesmo local)
with
    cadastro as (
        select
            ano,
            id_municipio,
            zona,
            secao,
            numero as numero_local,
            nome as nome_local,
            endereco,
            tipo_secao_agregada = '2' as indicador_secao_agregada,
            eleitores_secao,
            -- coordenadas nulas ou zeradas contam como ausentes
            if(
                latitude is not null
                and longitude is not null
                and latitude != 0
                and longitude != 0,
                st_geogpoint(longitude, latitude),
                null
            ) as ponto
        from {{ source("basedosdados_br_tse_eleicoes", "perfil_eleitorado_local_votacao") }}
        where turno = 1 and sigla_uf = 'RJ' and ano in (2022, 2024, 2026)
    ),

    pontos_2024_secao as (
        select id_municipio, zona, secao, st_centroid_agg(ponto) as ponto
        from cadastro
        where ano = 2024 and ponto is not null
        group by id_municipio, zona, secao
    ),

    pontos_2024_local as (
        select
            id_municipio,
            zona,
            upper(trim(nome_local)) as nome_local,
            upper(trim(endereco)) as endereco,
            st_centroid_agg(ponto) as ponto
        from cadastro
        where ano = 2024 and ponto is not null
        group by id_municipio, zona, upper(trim(nome_local)), upper(trim(endereco))
    ),

    secoes as (
        select
            c.ano,
            {{ id_secao("c.id_municipio", "c.zona", "c.secao") }} as id_secao,
            {{ id_zona("c.id_municipio", "c.zona") }} as id_zona,
            safe_cast(c.secao as int64) as secao,
            {{ id_local_votacao("c.id_municipio", "c.zona", "c.numero_local") }} as id_local_votacao,
            c.indicador_secao_agregada,
            c.eleitores_secao,
            coalesce(c.ponto, ps.ponto, pl.ponto) as geometry,
            case
                when c.ponto is not null
                then 'tse'
                when ps.ponto is not null
                then 'herdada_secao'
                when pl.ponto is not null
                then 'herdada_local'
            end as metodo_geocodificacao
        from cadastro as c
        left join
            pontos_2024_secao as ps
            on c.id_municipio = ps.id_municipio
            and c.zona = ps.zona
            and c.secao = ps.secao
        left join
            pontos_2024_local as pl
            on c.id_municipio = pl.id_municipio
            and c.zona = pl.zona
            and upper(trim(c.nome_local)) = pl.nome_local
            and upper(trim(c.endereco)) = pl.endereco
    )

select
    ano,
    id_secao,
    id_zona,
    secao,
    id_local_votacao,
    indicador_secao_agregada,
    eleitores_secao,
    st_y(geometry) as latitude,
    st_x(geometry) as longitude,
    geometry,
    metodo_geocodificacao
from secoes
