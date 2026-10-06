{{
    config(
        schema="app_eleicoes",
        alias="fct_votos_zona",
        materialized="table",
        cluster_by=["ano", "cargo", "turno"],
        tags=["app_eleicoes"],
    )
}}

-- votos por candidato em cada zona, somados a partir das seções (boletim de urna).
-- a tabela de resultados por zona do TSE perde candidatos (ex.: governador 2026, 274 mil
-- votos do Republicanos aparecem só nas seções), então não é usada.
-- a proporção sobre votos válidos usa a soma dos votos dos candidatos na zona como
-- denominador, assim as proporções da zona fecham em 1
select
    ano,
    turno,
    cargo,
    id_zona,
    id_candidato,
    any_value(numero_candidato) as numero_candidato,
    any_value(sigla_partido) as sigla_partido,
    sum(votos) as votos,
    safe_divide(
        sum(votos), sum(sum(votos)) over (partition by ano, turno, cargo, id_zona)
    ) as proporcao_votos_validos,
    safe_divide(sum(votos), any_value(c.aptos)) as proporcao_aptos
from {{ ref("mart_app_eleicoes__fct_votos_secao") }} as v
left join
    {{ ref("mart_app_eleicoes__fct_comparecimento_zona") }} as c using (ano, turno, cargo, id_zona)
group by ano, turno, cargo, id_zona, id_candidato
