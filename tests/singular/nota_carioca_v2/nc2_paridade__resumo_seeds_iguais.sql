-- Paridade F1 sobre as seeds "iguais": todas as 11 regras (R-01 a R-11) comparam algo
-- (comparados > 0), nenhuma tem divergente e só aparecem as correções catalogadas
-- esperadas (B-04 em R-06, R-07 e R-11; B-06 em R-08, R-09 e R-11). O corte é o maior
-- NSU do Oracle (8000). Contrato do resumo: uma linha por regra e classe, com as 5
-- colunas do flow rj_smfp__nota_carioca_paridade_f1.
with
    regras as (
        select regra
        from
            unnest(
                [
                    'R-01',
                    'R-02',
                    'R-03',
                    'R-04',
                    'R-05',
                    'R-06',
                    'R-07',
                    'R-08',
                    'R-09',
                    'R-10',
                    'R-11'
                ]
            ) as regra
    ),

    resumo as (select * from {{ ref("paridade_nc2__resumo") }}),

    por_regra as (
        select
            r.regra,
            coalesce(sum(s.quantidade), 0) as comparados,
            coalesce(
                sum(if(s.classe = 'divergente', s.quantidade, 0)), 0
            ) as divergentes
        from regras as r
        left join resumo as s on s.regra = r.regra
        group by r.regra
    ),

    correcoes_esperadas as (
        select 'R-06' as regra, 'correcao_catalogada:B-04' as classe, 4 as quantidade
        union all
        select 'R-07', 'correcao_catalogada:B-04', 4
        union all
        select 'R-08', 'correcao_catalogada:B-06', 2
        union all
        select 'R-09', 'correcao_catalogada:B-06', 1
        union all
        select 'R-11', 'correcao_catalogada:B-04', 6
        union all
        select 'R-11', 'correcao_catalogada:B-06', 1
    )

select
    regra,
    'comparados = 0: a regra não comparou nada' as falha,
    cast(comparados as string) as obtido
from por_regra
where comparados = 0
union all
select regra, 'divergente > 0 nas seeds iguais', cast(divergentes as string)
from por_regra
where divergentes > 0
union all
select
    coalesce(e.regra, s.regra),
    concat(
        'correção catalogada ', coalesce(e.classe, s.classe), ' diferente do esperado'
    ),
    concat(
        'esperado ',
        coalesce(cast(e.quantidade as string), '0'),
        ' obtido ',
        coalesce(cast(s.quantidade as string), '0')
    )
from correcoes_esperadas as e
full outer join
    (select * from resumo where classe like 'correcao_catalogada:%') as s
    on s.regra = e.regra
    and s.classe = e.classe
where e.quantidade is distinct from s.quantidade
union all
select
    regra,
    'corte de NSU diferente do maior NSU do Oracle (8000)',
    cast(corte_nsu as string)
from resumo
where corte_nsu is distinct from 8000
union all
select 'resumo', 'uma linha por regra e classe', cast(count(*) as string)
from
    (
        select regra, classe, count(*) as linhas
        from resumo
        group by regra, classe
        having count(*) > 1
    )
having count(*) > 0
