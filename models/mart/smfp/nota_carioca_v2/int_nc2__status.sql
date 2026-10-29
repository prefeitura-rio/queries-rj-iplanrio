{{ config(cluster_by=["chave_acesso"]) }}

-- Status final de cada nota: R-08 (eventos) e depois R-09 (substituição), nessa
-- ordem, como RPE_NOTASNACIONAIS.pkb. STATUS_NOTA: 0 normal, 1 cancelada, 2
-- bloqueada. Sem evento nem substituição a nota é normal (0). A substituição (Regra
-- 2) só vale se a nota substituída não está cancelada ou ainda não tem
-- CHAVE_ACESSO_SUBSTITUTA (um e105102 com a chave da substituta tem precedência); aí
-- ela vira 1, com DATA_CANCELAMENTO = DATA_VALIDACAO da substituta e
-- CHAVE_ACESSO_SUBSTITUTA = a chave da substituta. origem_status: 'normal', 'evento'
-- ou 'substituicao'. nsu_max_evento e nsu_substituta ficam para a paridade.
with
    combinado as (
        select
            n.chave_acesso,
            n.nsu,
            coalesce(e.status_nota, 0) as status_evento,
            e.data_cancelamento as data_cancelamento_evento,
            e.chave_acesso_substituta as substituta_evento,
            e.chave_acesso is not null as tem_evento,
            e.nsu_max_evento,
            s.chave_acesso_substituta as substituta_nota,
            s.data_cancelamento as data_cancelamento_substituta,
            s.nsu_substituta,
            (
                s.chave_acesso is not null
                and (
                    coalesce(e.status_nota, 0) <> 1 or e.chave_acesso_substituta is null
                )
            ) as aplica_substituicao
        from {{ ref("int_nc2__dedup") }} as n
        left join
            {{ ref("int_nc2__eventos_status") }} as e on e.chave_acesso = n.chave_acesso
        left join
            {{ ref("int_nc2__substituicao") }} as s on s.chave_acesso = n.chave_acesso
    )

select
    chave_acesso,
    nsu,
    if(aplica_substituicao, 1, status_evento) as status_nota,
    if(
        aplica_substituicao, data_cancelamento_substituta, data_cancelamento_evento
    ) as data_cancelamento,
    if(
        aplica_substituicao, substituta_nota, substituta_evento
    ) as chave_acesso_substituta,
    case
        when aplica_substituicao
        then 'substituicao'
        when tem_evento
        then 'evento'
        else 'normal'
    end as origem_status,
    nsu_max_evento,
    nsu_substituta
from combinado
