-- R-08: vale o evento mais recente (DATA_EVENTO desc, NSU desc) entre e101101,
-- e105104, e305101, e105102, e305102 e e305103; outros eventos não mudam o status.
-- Nunca a regra do VB (primeiro do lote, menor NSU): C07 e C08 só passam com a regra
-- do PL/SQL. Cancelamento define DATA_CANCELAMENTO; e105102 define também a chave da
-- substituta.
with
    esperado as (
        select
            'C01_CANCELA' as chave_acesso,
            1 as status_nota,
            datetime '2026-06-20 08:00:00' as data_cancelamento,
            cast(null as string) as chave_acesso_substituta
        union all
        select 'C02_CANCELA_ANALISE', 1, datetime '2026-06-20 08:05:00', null
        union all
        select 'C03_CANCELA_OFICIO', 1, datetime '2026-06-20 08:10:00', null
        union all
        select 'C04_BLOQUEIO', 2, null, null
        union all
        select 'C05_BLOQ_DESBLOQ', 0, null, null
        union all
        select 'C06_DESBLOQ_BLOQ', 2, null, null
        union all
        select 'C07_LOTE', 1, datetime '2026-06-22 09:00:00', null
        union all
        select 'C08_EMPATE', 0, null, null
        union all
        select 'C09_IRRELEVANTE', 0, null, null
        union all
        select 'C10_SUBST_EVENTO', 1, datetime '2026-06-23 09:00:00', 'C10_NOVA_CHAVE'
        union all
        select 'C11_CANCELA_DESBLOQ', 0, null, null
        union all
        select 'A01_NAO_RETIDA', 0, null, null
    )

select
    e.chave_acesso,
    'status, DATA_CANCELAMENTO ou CHAVE_ACESSO_SUBSTITUTA diferentes do esperado'
    as falha,
    concat(
        e.status_nota,
        '|',
        coalesce(cast(e.data_cancelamento as string), ''),
        '|',
        coalesce(e.chave_acesso_substituta, '')
    ) as esperado,
    concat(
        s.status_nota,
        '|',
        coalesce(cast(s.data_cancelamento as string), ''),
        '|',
        coalesce(s.chave_acesso_substituta, '')
    ) as obtido
from esperado as e
left join {{ ref("int_nc2__status") }} as s on s.chave_acesso = e.chave_acesso
where
    s.status_nota is distinct from e.status_nota
    or s.data_cancelamento is distinct from e.data_cancelamento
    or s.chave_acesso_substituta is distinct from e.chave_acesso_substituta
union all
select
    chave_acesso,
    'bloqueio seguido de desbloqueio dá status 0 e a nota é exigível',
    '0',
    cast(status_nota as string)
from {{ ref("int_nc2__status") }}
where chave_acesso = 'C05_BLOQ_DESBLOQ' and status_nota <> 0
union all
select
    'C07_LOTE',
    'dois eventos no mesmo lote: a regra do VB (menor NSU, bloqueio = 2) não pode vencer',
    '1',
    cast(status_nota as string)
from {{ ref("int_nc2__status") }}
where chave_acesso = 'C07_LOTE' and status_nota = 2
union all
select
    chave_acesso,
    'origem do status: normal sem evento relevante, evento nos demais',
    'normal/evento',
    origem_status
from {{ ref("int_nc2__status") }}
where
    (
        chave_acesso in ('C09_IRRELEVANTE', 'A01_NAO_RETIDA')
        and origem_status <> 'normal'
    )
    or (chave_acesso = 'C07_LOTE' and origem_status <> 'evento')
union all
select chave_acesso, 'evento de nota que não existe não cria linha de status', '0', '1'
from {{ ref("int_nc2__status") }}
where chave_acesso = 'CHAVE_SEM_NOTA'
union all
select
    chave_acesso,
    'maior NSU de evento relevante da nota (insumo do corte da paridade)',
    '9002',
    cast(nsu_max_evento as string)
from {{ ref("int_nc2__status") }}
where
    chave_acesso = 'L01_CANCELADA_ALEM_DO_CORTE'
    and nsu_max_evento is distinct from 9002
