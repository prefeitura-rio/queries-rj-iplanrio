{{
    config(
        schema="app_eleicoes",
        alias="fct_comparecimento_secao",
        materialized="table",
        cluster_by=["ano", "cargo", "turno", "id_zona"],
        tags=["app_eleicoes"],
    )
}}

-- comparecimento e abstenção por seção. o dado por seção não traz votos válidos, que
-- são os votos nominais mais os de legenda
select
    ano,
    turno,
    cargo,
    {{ id_secao("id_municipio", "zona", "secao") }} as id_secao,
    {{ id_zona("id_municipio", "zona") }} as id_zona,
    aptos,
    comparecimento,
    abstencoes,
    votos_nominais + votos_legenda as votos_validos,
    votos_brancos,
    votos_nulos,
    votos_nominais,
    votos_legenda,
    safe_divide(comparecimento, aptos) as proporcao_comparecimento,
    safe_divide(abstencoes, aptos) as proporcao_abstencao,
    safe_divide(votos_nominais + votos_legenda, comparecimento) as proporcao_votos_validos,
    safe_divide(votos_brancos, comparecimento) as proporcao_votos_brancos,
    safe_divide(votos_nulos, comparecimento) as proporcao_votos_nulos
from {{ source("basedosdados_br_tse_eleicoes", "detalhes_votacao_secao") }}
where sigla_uf = 'RJ' and {{ filtro_escopo_eleicoes() }}
