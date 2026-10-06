{{
    config(
        schema="app_eleicoes",
        alias="dim_candidato",
        materialized="table",
        cluster_by=["ano", "cargo"],
        tags=["app_eleicoes"],
    )
}}

-- candidaturas do recorte. presidente é nacional (em 2022 vem sem UF), então só os
-- demais cargos são filtrados por UF
with
    candidatos as (
        select
            ano,
            cargo,
            sequencial,
            numero,
            nome,
            nome_urna,
            sigla_partido,
            titulo_eleitoral,
            situacao
        from {{ source("basedosdados_br_tse_eleicoes", "candidatos") }}
        where {{ filtro_escopo_eleicoes() }} and (cargo = 'presidente' or sigla_uf = 'RJ')
    )

select
    concat(c.ano, '-', c.cargo, '-', c.sequencial) as id_candidato,
    c.ano,
    c.cargo,
    c.sequencial,
    c.numero,
    c.nome,
    c.nome_urna,
    c.sigla_partido,
    c.titulo_eleitoral,
    c.situacao
from candidatos as c
