-- R-09 (só a marcação da nota substituída): (STATUS_NOTA, DATA_CANCELAMENTO,
-- CHAVE_ACESSO_SUBSTITUTA) das notas que alguma nota substitui
-- (CHAVE_ACESSO_SUBSTITUIDA preenchida em algum dos lados), DFEN.NOTAS_NACIONAIS
-- contra int_nc2__status. NSU <= corte, e fora as notas que o v2 já alterou com NSU
-- acima do corte. Crédito de ISS e guia substituta ficam no Oracle e não entram aqui.
-- Correção catalogada B-06/R-09: status e chave da substituta iguais e o Oracle
-- gravou como DATA_CANCELAMENTO a DATA_EMISSAO da DPS da substituta (VB,
-- NotasNacionais.vb:2150), enquanto o v2 usa a DATA_VALIDACAO da substituta (PL/SQL).
-- Outra diferença é divergente.
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

    v2 as (
        select
            s.chave_acesso,
            s.status_nota,
            format_datetime(
                '%Y-%m-%dT%H:%M:%S', s.data_cancelamento
            ) as data_cancelamento,
            s.chave_acesso_substituta
        from {{ ref("int_nc2__status") }} as s
        inner join substituidas as b on b.chave_acesso = s.chave_acesso
        where
            s.nsu <= {{ nc2_corte() }}
            and not exists (
                select 1 from afetadas as a where a.chave_acesso = s.chave_acesso
            )
    ),

    oracle as (
        select
            n.chave_acesso,
            cast(n.status_nota as int64) as status_nota,
            n.data_cancelamento,
            n.chave_acesso_substituta
        from {{ nc2_fonte("nota_carioca", "NOTAS_NACIONAIS") }} as n
        inner join substituidas as b on b.chave_acesso = n.chave_acesso
        where
            n.nsu <= {{ nc2_corte() }}
            and not exists (
                select 1 from afetadas as a where a.chave_acesso = n.chave_acesso
            )
    ),

    emissao_substituta as (
        select
            chave_acesso,
            format_datetime(
                '%Y-%m-%dT%H:%M:%S', data_emissao_substituta
            ) as data_emissao_substituta
        from {{ ref("int_nc2__substituicao") }}
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
                v.status_nota is not distinct from o.status_nota
                and v.chave_acesso_substituta
                is not distinct from o.chave_acesso_substituta
                and o.data_cancelamento is not distinct from e.data_emissao_substituta
            ) as data_do_vb
        from v2 as v
        full outer join oracle as o on o.chave_acesso = v.chave_acesso
        left join
            emissao_substituta as e
            on e.chave_acesso = coalesce(v.chave_acesso, o.chave_acesso)
    )

select
    'R-09' as regra,
    chave,
    case
        when no_v2 and no_oracle and colunas_iguais
        then 'igual'
        when no_v2 and no_oracle and data_do_vb
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
