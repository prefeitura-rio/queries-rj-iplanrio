{{
    config(
        schema="app_eleicoes",
        alias="dim_local_votacao",
        materialized="table",
        cluster_by=["ano", "id_zona"],
        tags=["app_eleicoes"],
    )
}}

-- locais de votação por ano. atributos vêm do cadastro do TSE e o ponto é o centroide
-- das seções do local, já com a herança de coordenadas feita em dim_secao.
-- id_bairro (só no Rio) vem do bairro que contém o ponto
with
    cadastro as (
        select
            ano,
            {{ id_local_votacao("id_municipio", "zona", "numero") }} as id_local_votacao,
            any_value(numero) as numero_local,
            any_value(nome) as nome_local,
            any_value(endereco) as endereco,
            any_value(bairro) as bairro,
            any_value(cep) as cep
        from {{ source("basedosdados_br_tse_eleicoes", "perfil_eleitorado_local_votacao") }}
        where turno = 1 and sigla_uf = 'RJ' and ano in (2022, 2024, 2026)
        group by ano, id_local_votacao
    ),

    secoes_por_local as (
        select
            ano,
            id_zona,
            id_local_votacao,
            count(*) as quantidade_secoes,
            sum(eleitores_secao) as eleitores,
            st_centroid_agg(geometry) as geometry,
            -- método de menor confiança entre as seções que têm coordenada
            max(
                case metodo_geocodificacao
                    when 'tse'
                    then 1
                    when 'herdada_secao'
                    then 2
                    when 'herdada_local'
                    then 3
                end
            ) as nivel_metodo
        from {{ ref("mart_app_eleicoes__dim_secao") }}
        group by ano, id_zona, id_local_votacao
    )

select
    s.ano,
    s.id_local_votacao,
    s.id_zona,
    c.numero_local,
    c.nome_local,
    c.endereco,
    c.bairro,
    b.id_bairro,
    c.cep,
    s.quantidade_secoes,
    s.eleitores,
    st_y(s.geometry) as latitude,
    st_x(s.geometry) as longitude,
    s.geometry,
    case
        s.nivel_metodo
        when 1
        then 'tse'
        when 2
        then 'herdada_secao'
        when 3
        then 'herdada_local'
    end as metodo_geocodificacao
from secoes_por_local as s
left join cadastro as c on s.ano = c.ano and s.id_local_votacao = c.id_local_votacao
left join
    {{ ref("mart_app_eleicoes__dim_bairro") }} as b
    on starts_with(s.id_zona, b.id_municipio)
    and st_contains(b.geometry, s.geometry)
