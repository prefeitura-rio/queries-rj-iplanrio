-- Corte por NSU: o que o raw do parser já tem acima do maior NSU do Oracle (8000) não
-- entra em comparação nenhuma, nem vira divergência. K01 (NSU 9001) é exigível no v2
-- e não existe no Oracle; M02 (NSU 9003) substitui M01; o cancelamento de L01 tem NSU
-- 9002. K01 e M02 ficam fora de todas as regras. M01 e L01 são notas do Oracle (NSU
-- até o corte) e entram em R-01, R-02 e R-06, mas saem de R-08, R-09 e R-11, onde o
-- v2 já viu um evento ou uma substituta que o Oracle ainda não viu.
with
    chaves as (
        select 'R-01' as regra, chave
        from {{ ref("paridade_nc2__r01") }}
        union all
        select 'R-02', chave
        from {{ ref("paridade_nc2__r02") }}
        union all
        select 'R-04', chave
        from {{ ref("paridade_nc2__r04") }}
        union all
        select 'R-06', chave
        from {{ ref("paridade_nc2__r06") }}
        union all
        select 'R-08', chave
        from {{ ref("paridade_nc2__r08") }}
        union all
        select 'R-09', chave
        from {{ ref("paridade_nc2__r09") }}
        union all
        select 'R-10', chave
        from {{ ref("paridade_nc2__r10") }}
        union all
        select 'R-11', chave
        from {{ ref("paridade_nc2__r11") }}
    ),

    nomes as (
        select 'K01_ACIMA_DO_CORTE' as nome, true as acima_do_corte
        union all
        select 'M02_SUBSTITUTA_ALEM_DO_CORTE', true
        union all
        select 'M01_SUBSTITUIDA_ALEM_DO_CORTE', false
        union all
        select 'L01_CANCELADA_ALEM_DO_CORTE', false
    )

select c.regra, c.chave, 'nota com NSU acima do corte entrou na comparação' as falha
from chaves as c
inner join
    nomes as n
    on n.acima_do_corte
    and c.chave in (
        concat('NFS', n.nome),
        n.nome,
        concat('D|NFS', n.nome),
        concat('NOTA|NFS', n.nome)
    )
union all
select
    c.regra,
    c.chave,
    'nota afetada por evento ou substituta acima do corte não pode ser comparada'
from chaves as c
inner join
    nomes as n
    on not n.acima_do_corte
    and c.regra in ('R-08', 'R-09', 'R-11')
    and c.chave in (n.nome, concat('D|NFS', n.nome))
union all
select
    esperadas.regra,
    esperadas.nome as chave,
    'nota do Oracle (NSU até o corte) ficou fora de R-01, R-02 ou R-06'
from
    (
        select regra, nome
        from nomes
        cross join unnest(['R-01', 'R-02', 'R-06']) as regra
        where not acima_do_corte
    ) as esperadas
left join
    chaves as c
    on c.regra = esperadas.regra
    and c.chave in (concat('NFS', esperadas.nome), esperadas.nome)
where c.chave is null
union all
select regra, chave, 'evento de NSU 9002 entrou em R-04'
from chaves
where regra = 'R-04' and chave = 'EVENTO|EVT_L01'
