-- R-04: conjuntos que sobram depois da deduplicação (menor NSU) e do corte
-- IniExtDocsFiscais, por entidade: NOTA_NACIONAL (com o NSU), EVENTO (com o NSU) e
-- DPS (só a presença: o Oracle não guarda o NSU da DPS), entre o Oracle
-- (NOTAS_NACIONAIS, EVENTOS_DFEN e DPS) e int_nc2__dedup / int_nc2__dedup_eventos.
-- Comparar o que sobrou equivale a comparar o que foi descartado. Só NSU <= corte; a
-- DPS do Oracle entra pelas notas dela.
with
    v2 as (
        select 'NOTA' as entidade, nota_nacional as id, nsu
        from {{ ref("int_nc2__dedup") }}
        where nsu <= {{ nc2_corte() }}
        union all
        select 'DPS' as entidade, dps as id, cast(null as int64) as nsu
        from {{ ref("int_nc2__dedup") }}
        where nsu <= {{ nc2_corte() }}
        union all
        select 'EVENTO' as entidade, evento as id, nsu
        from {{ ref("int_nc2__dedup_eventos") }}
        where evento is not null and nsu <= {{ nc2_corte() }}
    ),

    oracle as (
        select 'NOTA' as entidade, nota_nacional as id, cast(nsu as int64) as nsu
        from {{ nc2_fonte("nota_carioca", "NOTAS_NACIONAIS") }}
        where nsu <= {{ nc2_corte() }}
        union all
        select distinct 'DPS' as entidade, dps as id, cast(null as int64) as nsu
        from {{ nc2_fonte("nota_carioca", "NOTAS_NACIONAIS") }}
        where nsu <= {{ nc2_corte() }}
        union all
        select 'EVENTO' as entidade, evento as id, cast(nsu as int64) as nsu
        from {{ nc2_fonte("nota_carioca", "EVENTOS_DFEN") }}
        where evento is not null and nsu <= {{ nc2_corte() }}
    ),

    comparado as (
        select
            concat(
                coalesce(v.entidade, o.entidade), '|', coalesce(v.id, o.id)
            ) as chave,
            v.id is not null as no_v2,
            o.id is not null as no_oracle,
            v.nsu is not distinct from o.nsu as nsu_igual
        from v2 as v
        full outer join oracle as o on o.entidade = v.entidade and o.id = v.id
    )

select
    'R-04' as regra,
    chave,
    if(no_v2 and no_oracle and nsu_igual, 'igual', 'divergente') as classe,
    case
        when not no_oracle
        then 'so_v2'
        when not no_v2
        then 'so_oracle'
        when not nsu_igual
        then 'nsu_diferente'
    end as detalhe
from comparado
