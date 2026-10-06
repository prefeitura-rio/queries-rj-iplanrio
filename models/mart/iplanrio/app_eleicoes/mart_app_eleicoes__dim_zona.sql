{{
    config(
        schema="app_eleicoes",
        alias="dim_zona",
        materialized="table",
        cluster_by=["id_municipio"],
        tags=["app_eleicoes"],
    )
}}

-- zonas eleitorais do RJ presentes em ao menos uma eleição do recorte
with
    zonas as (
        select
            ano,
            sigla_uf,
            id_municipio,
            id_municipio_tse,
            safe_cast(zona as int64) as zona
        from {{ source("basedosdados_br_tse_eleicoes", "detalhes_votacao_municipio_zona") }}
        where sigla_uf = 'RJ' and {{ filtro_escopo_eleicoes() }}
    ),

    zonas_agrupadas as (
        select
            sigla_uf,
            id_municipio,
            id_municipio_tse,
            zona,
            array_agg(distinct ano order by ano) as anos
        from zonas
        group by sigla_uf, id_municipio, id_municipio_tse, zona
    ),

    municipios as (
        select
            id_municipio,
            nome as nome_municipio,
            capital_uf = 1 as indicador_capital,
            nome_regiao_metropolitana
        from {{ source("basedosdados_br_bd_diretorios_brasil", "municipio") }}
        where sigla_uf = 'RJ'
    )

select
    {{ id_zona("z.id_municipio", "z.zona") }} as id_zona,
    z.sigla_uf,
    z.id_municipio,
    z.id_municipio_tse,
    m.nome_municipio,
    z.zona,
    m.indicador_capital,
    m.nome_regiao_metropolitana,
    z.anos
from zonas_agrupadas as z
left join municipios as m on z.id_municipio = m.id_municipio
