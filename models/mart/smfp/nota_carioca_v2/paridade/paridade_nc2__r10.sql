-- R-10: COALESCE(FISCALIZACAO, 0) por CHAVE_ACESSO, DFEN.NOTAS_NACIONAIS contra
-- int_nc2__fiscalizacao (origem_replicada: o v2 lê a mesma réplica do Oracle, então
-- esta regra só acusa nota que falta de um lado ou valor perdido no caminho).
-- Universo: notas com FISCALIZACAO diferente de 0 em algum dos lados (matriz: todas
-- as notas com FISCALIZACAO em 1 ou 2). NSU <= corte.
with
    v2 as (
        select chave_acesso as chave, fiscalizacao
        from {{ ref("int_nc2__fiscalizacao") }}
        where nsu <= {{ nc2_corte() }} and fiscalizacao <> 0
    ),

    oracle as (
        select
            chave_acesso as chave,
            coalesce(cast(fiscalizacao as int64), 0) as fiscalizacao
        from {{ nc2_fonte("nota_carioca", "NOTAS_NACIONAIS") }}
        where nsu <= {{ nc2_corte() }} and coalesce(cast(fiscalizacao as int64), 0) <> 0
    ),

    comparado as (
        select
            coalesce(v.chave, o.chave) as chave,
            v.chave is not null as no_v2,
            o.chave is not null as no_oracle,
            v.fiscalizacao is not distinct from o.fiscalizacao as colunas_iguais
        from v2 as v
        full outer join oracle as o on o.chave = v.chave
    )

select
    'R-10' as regra,
    chave,
    if(no_v2 and no_oracle and colunas_iguais, 'igual', 'divergente') as classe,
    case
        when not no_oracle
        then 'so_v2'
        when not no_v2
        then 'so_oracle'
        when not colunas_iguais
        then 'colunas_diferentes'
    end as detalhe
from comparado
