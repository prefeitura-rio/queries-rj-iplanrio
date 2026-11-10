-- R-06: CENARIO_EXIGIBILIDADE por NOTA_NACIONAL, DFEN.NOTAS_NACIONAIS (já
-- classificado por VB e PL/SQL) contra int_nc2__cenario, para as notas com
-- competência municipal >= InicioPlanoB (matriz: notas de 05/2026 em diante), NSU <=
-- corte. Diferença esperada e catalogada (B-04): o Oracle tem -1 ou ainda nulo (VB:
-- Nothing; PL/SQL: -1 antes de 4 e 5, preso depois de 7 dias, e também para ISS
-- retido) e o v2 classifica pela cascata corrigida; ou o Oracle e o v2 divergem na
-- ordem entre 1, 2 e 3 (VB avalia 2, 3, 1; PL/SQL 1, 2, 3). Qualquer outra diferença
-- é divergente.
with
    v2 as (
        select nota_nacional as chave, cenario_exigibilidade
        from {{ ref("int_nc2__cenario") }}
        where
            nsu <= {{ nc2_corte() }}
            and data_compmunicipio
            >= (select inicio_plano_b from {{ ref("int_nc2__config") }})
    ),

    oracle as (
        select
            nota_nacional as chave,
            cast(cenario_exigibilidade as int64) as cenario_exigibilidade
        from {{ nc2_fonte("nota_carioca", "NOTAS_NACIONAIS") }}
        where
            nsu <= {{ nc2_corte() }}
            and safe_cast(substr(data_compmunicipio, 1, 10) as date)
            >= (select inicio_plano_b from {{ ref("int_nc2__config") }})
    ),

    comparado as (
        select
            coalesce(v.chave, o.chave) as chave,
            v.chave is not null as no_v2,
            o.chave is not null as no_oracle,
            v.cenario_exigibilidade as cenario_v2,
            o.cenario_exigibilidade as cenario_oracle
        from v2 as v
        full outer join oracle as o on o.chave = v.chave
    )

select
    'R-06' as regra,
    chave,
    case
        when not (no_v2 and no_oracle)
        then 'divergente'
        when cenario_v2 is not distinct from cenario_oracle
        then 'igual'
        when cenario_oracle is null or cenario_oracle = -1
        then 'correcao_catalogada:B-04'
        when cenario_oracle in (1, 2, 3) and cenario_v2 in (1, 2, 3)
        then 'correcao_catalogada:B-04'
        else 'divergente'
    end as classe,
    concat(
        'oracle=',
        coalesce(cast(cenario_oracle as string), 'nulo'),
        ' v2=',
        coalesce(cast(cenario_v2 as string), 'ausente')
    ) as detalhe
from comparado
