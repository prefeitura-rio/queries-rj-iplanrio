-- R-01: a competência municipal é o primeiro dia do mês da competência do serviço,
-- mais 1 mês no ISS retido (tpRetISSQN 2 ou 3). A nota retida com competência
-- 2026-04-15 vai para 2026-05-01. Que ela continua sendo Plano C (cenário 4) é
-- conferido em nc2_r06_r07__cenario_exigibilidade: o Plano B usa a competência do
-- serviço.
with
    esperado as (
        select 'A01_NAO_RETIDA' as chave_acesso, date '2026-06-01' as data_compmunicipio
        union all
        select 'A02_RETIDA_ABRIL', date '2026-05-01'
        union all
        select 'A03_RETIDA_TOMADOR', date '2026-07-01'
        union all
        select 'A04_RETIDA_INTERMEDIARIO', date '2026-07-01'
        union all
        select 'A05_SEM_TOMADOR', date '2026-06-01'
        union all
        select 'B04_NO_INI', date '2026-01-01'
        union all
        select 'H18_RETIDA_HABILITADA', date '2026-07-01'
    )

select
    e.chave_acesso,
    'competência municipal diferente da esperada' as falha,
    cast(e.data_compmunicipio as string) as esperado,
    cast(c.data_compmunicipio as string) as obtido
from esperado as e
left join {{ ref("int_nc2__competencia") }} as c on c.chave_acesso = e.chave_acesso
where c.data_compmunicipio is distinct from e.data_compmunicipio
