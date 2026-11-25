-- R-04 sobre as seeds sintéticas: duplicata mantém só o menor NSU, reprocessamento
-- fica com a ingestão mais recente, competência anterior a IniExtDocsFiscais é
-- descartada (a de 2026-01-01 não), nota sem DPS não entra, evento e pessoa repetidos
-- ficam uma vez. Devolve uma linha por caso que falhou.
with
    notas as (select * from {{ ref("int_nc2__dedup") }}),

    eventos as (select * from {{ ref("int_nc2__dedup_eventos") }}),

    pessoas as (select * from {{ ref("int_nc2__dedup_pessoas") }}),

    pessoas_raw as (
        select hash_id, min(nsu) as menor_nsu, count(*) as ocorrencias
        from {{ ref("nc2_seed__raw_nfse_nacional__pessoas") }}
        group by hash_id
    ),

    casos as (
        select
            'duplicata de nota com NSUs 300 e 500 mantém só o NSU 300 (e o valor dele)'
            as caso,
            (select count(*) from notas where chave_acesso = 'B01_DUPLICATA_NSU') = 1
            and (select min(nsu) from notas where chave_acesso = 'B01_DUPLICATA_NSU')
            = 300
            and (
                select min(valor_issqn)
                from notas
                where chave_acesso = 'B01_DUPLICATA_NSU'
            )
            = 100 as ok
        union all
        select
            'a DPS também fica com a ocorrência de menor NSU',
            (select min(nsu_dps) from notas where chave_acesso = 'B01_DUPLICATA_NSU')
            = 300
        union all
        select
            'mesmo NSU reprocessado pelo parser: vale a ingestão mais recente',
            (select count(*) from notas where chave_acesso = 'B02_REPROCESSADA') = 1
            and (
                select min(valor_issqn)
                from notas
                where chave_acesso = 'B02_REPROCESSADA'
            )
            = 100
        union all
        select
            'competência 2025-12-20 (antes de IniExtDocsFiscais) é descartada',
            (select count(*) from notas where chave_acesso = 'B03_ANTES_INI') = 0
        union all
        select
            'competência 2026-01-01 (igual a IniExtDocsFiscais) fica',
            (select count(*) from notas where chave_acesso = 'B04_NO_INI') = 1
        union all
        select
            'nota sem DPS no raw não entra',
            (select count(*) from notas where chave_acesso = 'B05_SEM_DPS') = 0
        union all
        select
            'evento repetido (EVT_DUP, NSUs 5080 e 5081) fica uma vez, com o menor NSU',
            (select count(*) from eventos where evento = 'EVT_DUP') = 1
            and (select min(nsu) from eventos where evento = 'EVT_DUP') = 5080
        union all
        select
            'toda pessoa do raw aparece uma vez, com o menor NSU dela',
            (
                select count(*)
                from pessoas_raw as r
                left join pessoas as p on p.hash_id = r.hash_id
                where p.hash_id is null or p.nsu <> r.menor_nsu
            )
            = 0
            and (select count(*) from pessoas) = (select count(*) from pessoas_raw)
        union all
        select
            'a pessoa citada em várias notas tem mais de uma linha no raw (o caso é real)',
            (select max(ocorrencias) from pessoas_raw) > 1
    )

select caso
from casos
where ok is not true
