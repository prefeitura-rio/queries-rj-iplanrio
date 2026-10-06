{{
    config(
        schema="app_eleicoes",
        alias="fct_votos_secao",
        materialized="table",
        cluster_by=["ano", "cargo", "turno", "id_zona"],
        tags=["app_eleicoes"],
    )
}}

-- votos por candidato em cada seção. seções agregadas não aparecem: seus votos estão na
-- seção principal.
-- o sequencial do resultado às vezes é nulo ou antigo (não existe em candidatos). nesses
-- casos o candidato é achado pelo número, que é único por cargo (e por município, no
-- caso de prefeito). se o número for ambíguo (substituição de candidato), a linha sai
with
    resultados as (
        select
            ano,
            turno,
            cargo,
            id_municipio,
            zona,
            secao,
            sequencial_candidato,
            numero_candidato,
            sigla_partido,
            votos,
            if(cargo = 'prefeito', id_municipio, '') as chave_local
        from {{ source("basedosdados_br_tse_eleicoes", "resultados_candidato_secao") }}
        where sigla_uf = 'RJ' and {{ filtro_escopo_eleicoes() }}
    ),

    candidatos as (
        select
            ano,
            cargo,
            sequencial,
            numero,
            if(cargo = 'prefeito', id_municipio, '') as chave_local
        from {{ source("basedosdados_br_tse_eleicoes", "candidatos") }}
        where {{ filtro_escopo_eleicoes() }} and (cargo = 'presidente' or sigla_uf = 'RJ')
    ),

    numero_unico as (
        select ano, cargo, chave_local, numero, min(sequencial) as sequencial
        from candidatos
        group by ano, cargo, chave_local, numero
        having count(*) = 1
    ),

    resolvidos as (
        select
            r.* except (sequencial_candidato),
            coalesce(c.sequencial, u.sequencial) as sequencial_candidato
        from resultados as r
        left join
            candidatos as c
            on r.ano = c.ano
            and r.cargo = c.cargo
            and r.sequencial_candidato = c.sequencial
        left join
            numero_unico as u
            on c.sequencial is null
            and r.ano = u.ano
            and r.cargo = u.cargo
            and r.chave_local = u.chave_local
            and r.numero_candidato = u.numero
    )

select
    ano,
    turno,
    cargo,
    {{ id_secao("id_municipio", "zona", "secao") }} as id_secao,
    {{ id_zona("id_municipio", "zona") }} as id_zona,
    concat(ano, '-', cargo, '-', sequencial_candidato) as id_candidato,
    numero_candidato,
    sigla_partido,
    votos
from resolvidos
where sequencial_candidato is not null
