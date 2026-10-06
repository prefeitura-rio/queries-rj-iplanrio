{{
    config(
        schema="app_eleicoes",
        alias="fct_comparecimento_zona",
        materialized="table",
        cluster_by=["ano", "cargo", "turno"],
        tags=["app_eleicoes"],
    )
}}

-- comparecimento e abstenção por zona. `secoes_totalizadas` vem zerado em 2026, então a
-- cobertura da apuração é calculada pelas seções que têm resultado.
-- votos nominais e válidos vêm da soma das seções (como fct_votos_zona): na tabela de
-- detalhes do TSE eles ficam abaixo do real (governador 2026 perde 274 mil votos)
with
    detalhes as (
        select
            ano,
            turno,
            cargo,
            id_municipio,
            zona,
            aptos,
            comparecimento,
            abstencoes,
            votos_brancos,
            votos_nulos,
            votos_legenda,
            secoes
        from {{ source("basedosdados_br_tse_eleicoes", "detalhes_votacao_municipio_zona") }}
        where sigla_uf = 'RJ' and {{ filtro_escopo_eleicoes() }}
    ),

    votos as (
        select ano, turno, cargo, id_zona, sum(votos) as votos_nominais
        from {{ ref("mart_app_eleicoes__fct_votos_secao") }}
        group by ano, turno, cargo, id_zona
    ),

    secoes_apuradas as (
        select
            ano,
            turno,
            cargo,
            id_municipio,
            zona,
            count(distinct secao) as secoes_apuradas
        from {{ source("basedosdados_br_tse_eleicoes", "resultados_candidato_secao") }}
        where sigla_uf = 'RJ' and {{ filtro_escopo_eleicoes() }}
        group by ano, turno, cargo, id_municipio, zona
    )

select
    d.ano,
    d.turno,
    d.cargo,
    {{ id_zona("d.id_municipio", "d.zona") }} as id_zona,
    d.aptos,
    d.comparecimento,
    d.abstencoes,
    coalesce(v.votos_nominais, 0) + d.votos_legenda as votos_validos,
    d.votos_brancos,
    d.votos_nulos,
    coalesce(v.votos_nominais, 0) as votos_nominais,
    d.votos_legenda,
    safe_divide(d.comparecimento, d.aptos) as proporcao_comparecimento,
    safe_divide(d.abstencoes, d.aptos) as proporcao_abstencao,
    safe_divide(coalesce(v.votos_nominais, 0) + d.votos_legenda, d.comparecimento) as proporcao_votos_validos,
    safe_divide(d.votos_brancos, d.comparecimento) as proporcao_votos_brancos,
    safe_divide(d.votos_nulos, d.comparecimento) as proporcao_votos_nulos,
    d.secoes,
    coalesce(s.secoes_apuradas, 0) as secoes_apuradas,
    safe_divide(coalesce(s.secoes_apuradas, 0), d.secoes) as proporcao_secoes_apuradas
from detalhes as d
left join
    votos as v
    on d.ano = v.ano
    and d.turno = v.turno
    and d.cargo = v.cargo
    and {{ id_zona("d.id_municipio", "d.zona") }} = v.id_zona
left join
    secoes_apuradas as s
    on d.ano = s.ano
    and d.turno = s.turno
    and d.cargo = s.cargo
    and d.id_municipio = s.id_municipio
    and d.zona = s.zona
