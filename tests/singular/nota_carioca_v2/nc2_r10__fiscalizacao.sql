-- R-10: COALESCE(fiscalizacao, 0) <> 2 a partir da réplica de NOTAS_NACIONAIS
-- (origem_replicada). Fiscalização 2 tira a nota das exigíveis; 1, nula e nota fora
-- da réplica ficam.
with
    esperado as (
        select 'I01_FISCALIZACAO_2' as chave_acesso, 2 as fiscalizacao
        union all
        select 'I02_FISCALIZACAO_1', 1
        union all
        select 'I03_FISCALIZACAO_NULA', 0
        union all
        select 'A01_NAO_RETIDA', 0
        union all
        select 'K01_ACIMA_DO_CORTE', 0
    )

select
    e.chave_acesso,
    'fiscalização diferente da esperada' as falha,
    cast(e.fiscalizacao as string) as esperado,
    cast(f.fiscalizacao as string) as obtido
from esperado as e
left join {{ ref("int_nc2__fiscalizacao") }} as f on f.chave_acesso = e.chave_acesso
where f.fiscalizacao is distinct from e.fiscalizacao
union all
select
    chave_acesso,
    'a regra é marcada como origem_replicada',
    'origem_replicada',
    origem_fiscalizacao
from {{ ref("int_nc2__fiscalizacao") }}
where origem_fiscalizacao is distinct from 'origem_replicada'
union all
select
    'I01_FISCALIZACAO_2',
    'nota com fiscalização 2 não é exigível',
    '0',
    cast(count(*) as string)
from {{ ref("mvt_notas_nacionais_exigiveis_detalhes") }}
where chave_acesso = 'I01_FISCALIZACAO_2'
having count(*) > 0
union all
select
    'I02_FISCALIZACAO_1',
    'fiscalização 1 (em andamento) continua exigível',
    '1',
    cast(count(*) as string)
from {{ ref("mvt_notas_nacionais_exigiveis_detalhes") }}
where chave_acesso in ('I02_FISCALIZACAO_1', 'I03_FISCALIZACAO_NULA')
having count(*) <> 2
