-- R-08: (STATUS_NOTA, DATA_CANCELAMENTO, CHAVE_ACESSO_SUBSTITUTA) por CHAVE_ACESSO,
-- DFEN.NOTAS_NACIONAIS (pós-Regra 1) contra int_nc2__status. Universo: notas com
-- evento ou status diferente do normal em algum dos lados, sem as substituídas
-- (R-09). Corte: ficam fora as notas com evento de status ou substituta de NSU acima
-- do corte, que o Oracle ainda não viu. Correção catalogada B-06: o Oracle é igual ao
-- que o VB faz (primeiro evento relevante do lote, o de menor NSU) e o v2 usa o mais
-- recente por DATA_EVENTO. Qualquer outra diferença é divergente.
with
    afetadas as (
        select chave_acesso
        from {{ ref("int_nc2__status") }}
        where
            coalesce(nsu_max_evento, 0) > {{ nc2_corte() }}
            or coalesce(nsu_substituta, 0) > {{ nc2_corte() }}
    ),

    substituidas as (
        select chave_acesso
        from {{ ref("int_nc2__substituicao") }}
        union distinct
        select chave_acesso_substituida as chave_acesso
        from {{ nc2_fonte("nota_carioca", "NOTAS_NACIONAIS") }}
        where chave_acesso_substituida is not null and nsu <= {{ nc2_corte() }}
    ),

    v2_todas as (
        select
            s.chave_acesso,
            s.status_nota,
            format_datetime(
                '%Y-%m-%dT%H:%M:%S', s.data_cancelamento
            ) as data_cancelamento,
            s.chave_acesso_substituta
        from {{ ref("int_nc2__status") }} as s
        where
            s.nsu <= {{ nc2_corte() }}
            and not exists (
                select 1 from afetadas as a where a.chave_acesso = s.chave_acesso
            )
            and not exists (
                select 1 from substituidas as b where b.chave_acesso = s.chave_acesso
            )
    ),

    oracle_todas as (
        select
            n.chave_acesso,
            cast(n.status_nota as int64) as status_nota,
            n.data_cancelamento,
            n.chave_acesso_substituta
        from {{ nc2_fonte("nota_carioca", "NOTAS_NACIONAIS") }} as n
        where
            n.nsu <= {{ nc2_corte() }}
            and not exists (
                select 1 from afetadas as a where a.chave_acesso = n.chave_acesso
            )
            and not exists (
                select 1 from substituidas as b where b.chave_acesso = n.chave_acesso
            )
    ),

    interessantes as (
        select chave_acesso
        from v2_todas
        where
            status_nota <> 0
            or data_cancelamento is not null
            or chave_acesso_substituta is not null
        union distinct
        select chave_acesso
        from oracle_todas
        where
            status_nota <> 0
            or data_cancelamento is not null
            or chave_acesso_substituta is not null
    ),

    v2 as (
        select t.*
        from v2_todas as t
        inner join interessantes as i on i.chave_acesso = t.chave_acesso
    ),

    oracle as (
        select t.*
        from oracle_todas as t
        inner join interessantes as i on i.chave_acesso = t.chave_acesso
    ),

    vb as (
        select
            chave_acesso,
            case
                when tipo_evento in ('e101101', 'e105104', 'e305101', 'e105102')
                then 1
                when tipo_evento = 'e305102'
                then 2
                when tipo_evento = 'e305103'
                then 0
            end as status_nota,
            if(
                tipo_evento in ('e101101', 'e105104', 'e305101', 'e105102'),
                format_datetime('%Y-%m-%dT%H:%M:%S', data_evento),
                null
            ) as data_cancelamento,
            if(
                tipo_evento = 'e105102', chave_acesso_substituta, null
            ) as chave_acesso_substituta
        from {{ ref("int_nc2__dedup_eventos") }}
        where
            chave_acesso is not null
            and tipo_evento
            in ('e101101', 'e105104', 'e305101', 'e105102', 'e305102', 'e305103')
        qualify row_number() over (partition by chave_acesso order by nsu asc) = 1
    ),

    comparado as (
        select
            coalesce(v.chave_acesso, o.chave_acesso) as chave,
            v.chave_acesso is not null as no_v2,
            o.chave_acesso is not null as no_oracle,
            (
                v.status_nota is not distinct from o.status_nota
                and v.data_cancelamento is not distinct from o.data_cancelamento
                and v.chave_acesso_substituta
                is not distinct from o.chave_acesso_substituta
            ) as colunas_iguais,
            (
                b.chave_acesso is not null
                and b.status_nota is not distinct from o.status_nota
                and b.data_cancelamento is not distinct from o.data_cancelamento
                and b.chave_acesso_substituta
                is not distinct from o.chave_acesso_substituta
            ) as oracle_igual_ao_vb
        from v2 as v
        full outer join oracle as o on o.chave_acesso = v.chave_acesso
        left join vb as b on b.chave_acesso = coalesce(v.chave_acesso, o.chave_acesso)
    )

select
    'R-08' as regra,
    chave,
    case
        when no_v2 and no_oracle and colunas_iguais
        then 'igual'
        when no_v2 and no_oracle and oracle_igual_ao_vb
        then 'correcao_catalogada:B-06'
        else 'divergente'
    end as classe,
    case
        when not no_oracle
        then 'so_v2'
        when not no_v2
        then 'so_oracle'
        when not colunas_iguais
        then 'colunas_diferentes'
    end as detalhe
from comparado
