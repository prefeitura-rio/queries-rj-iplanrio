-- R-09 (só a marcação da nota substituída): a nota substituída vira cancelada, com
-- DATA_CANCELAMENTO = DATA_VALIDACAO da substituta (correção catalogada B-06/R-09) e
-- a chave da substituta. Cadeia A <- B <- C marca A e B. Um e105102 com a chave da
-- substituta tem precedência; cancelada por e101101 (sem chave) ainda é marcada pela
-- substituta; alvo que não existe não gera nada.
with
    esperado as (
        select
            'D01_SUBSTITUIDA' as chave_acesso,
            1 as status_nota,
            datetime '2026-06-25 10:00:00' as data_cancelamento,
            'D02_SUBSTITUTA' as chave_acesso_substituta,
            'substituicao' as origem_status
        union all
        select
            'D03_SUBSTITUIDA',
            1,
            datetime '2026-06-26 11:30:00',
            'D04_SUBSTITUTA',
            'substituicao'
        union all
        select
            'E01_CADEIA_A',
            1,
            datetime '2026-06-27 09:00:00',
            'E02_CADEIA_B',
            'substituicao'
        union all
        select
            'E02_CADEIA_B',
            1,
            datetime '2026-06-28 09:00:00',
            'E03_CADEIA_C',
            'substituicao'
        union all
        select 'E03_CADEIA_C', 0, cast(null as datetime), cast(null as string), 'normal'
        union all
        select
            'F01_EVENTO_E_SUBST',
            1,
            datetime '2026-06-18 12:00:00',
            'F02_SUBSTITUTA_DE_F01',
            'evento'
        union all
        select
            'F03_CANCELADA_SEM_SUBST',
            1,
            datetime '2026-06-26 08:00:00',
            'F04_SUBSTITUTA_DE_F03',
            'substituicao'
        union all
        select 'D02_SUBSTITUTA', 0, null, null, 'normal'
        union all
        select 'G01_ALVO_INEXISTENTE', 0, null, null, 'normal'
    )

select
    e.chave_acesso,
    'status, DATA_CANCELAMENTO, chave da substituta ou origem diferentes do esperado'
    as falha,
    concat(
        e.status_nota,
        '|',
        coalesce(cast(e.data_cancelamento as string), ''),
        '|',
        coalesce(e.chave_acesso_substituta, ''),
        '|',
        e.origem_status
    ) as esperado,
    concat(
        s.status_nota,
        '|',
        coalesce(cast(s.data_cancelamento as string), ''),
        '|',
        coalesce(s.chave_acesso_substituta, ''),
        '|',
        s.origem_status
    ) as obtido
from esperado as e
left join {{ ref("int_nc2__status") }} as s on s.chave_acesso = e.chave_acesso
where
    s.status_nota is distinct from e.status_nota
    or s.data_cancelamento is distinct from e.data_cancelamento
    or s.chave_acesso_substituta is distinct from e.chave_acesso_substituta
    or s.origem_status is distinct from e.origem_status
union all
select
    chave_acesso,
    'a marcação da substituição não gera linha para alvo inexistente',
    '0',
    '1'
from {{ ref("int_nc2__substituicao") }}
where chave_acesso = 'CHAVE_QUE_NAO_EXISTE'
union all
select
    chave_acesso,
    'a substituta de menor NSU é a que vale: uma linha por nota substituída',
    '1',
    cast(count(*) as string)
from {{ ref("int_nc2__substituicao") }}
group by chave_acesso
having count(*) <> 1
