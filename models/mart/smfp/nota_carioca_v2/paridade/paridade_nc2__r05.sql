-- R-05: o que o parser leu do XML contra o que o .NET gravou. Universo: o que o
-- Oracle aceitou (NOTAS_NACIONAIS e EVENTOS_DFEN, NSU <= corte). Cada linha do Oracle
-- tem de existir no raw do parser (a nota sem deduplicação por competência, o evento
-- já deduplicado) com os mesmos valores, inclusive os monetários, sem tolerância.
-- Linhas só do raw (o .NET descartou por duplicidade ou competência) não são falha de
-- parse e ficam fora: são a R-04. Por isso o FULL OUTER JOIN termina filtrando o lado
-- do Oracle.
with
    v2 as (
        select
            concat('NOTA|', nota_nacional) as chave,
            to_json_string(
                struct(
                    cast(nota_fiscal as numeric) as nota_fiscal,
                    chave_acesso as chave_acesso,
                    dps as dps,
                    cod_local_incidencia as cod_local_incidencia,
                    valor_ded_red as valor_ded_red,
                    valor_base_calculo as valor_base_calculo,
                    aliquota as aliquota,
                    valor_issqn as valor_issqn,
                    format_datetime(
                        '%Y-%m-%dT%H:%M:%S', data_validacao
                    ) as data_validacao
                )
            ) as conteudo
        from
            (
                select *
                from {{ nc2_raw("nfse") }}
                where nota_nacional is not null and nsu <= {{ nc2_corte() }}
                qualify
                    row_number() over (
                        partition by nota_nacional order by nsu asc, ingerido_em desc
                    )
                    = 1
            )
        union all
        select
            concat('EVENTO|', evento) as chave,
            to_json_string(
                struct(
                    chave_acesso as chave_acesso,
                    tipo_evento as tipo_evento,
                    format_datetime('%Y-%m-%dT%H:%M:%S', data_evento) as data_evento,
                    chave_acesso_substituta as chave_acesso_substituta
                )
            ) as conteudo
        from {{ ref("int_nc2__dedup_eventos") }}
        where evento is not null and nsu <= {{ nc2_corte() }}
    ),

    oracle as (
        select
            concat('NOTA|', nota_nacional) as chave,
            to_json_string(
                struct(
                    nota_fiscal as nota_fiscal,
                    chave_acesso as chave_acesso,
                    dps as dps,
                    cod_local_incidencia as cod_local_incidencia,
                    valor_ded_red as valor_ded_red,
                    valor_base_calculo as valor_base_calculo,
                    aliquota as aliquota,
                    valor_issqn as valor_issqn,
                    data_validacao as data_validacao
                )
            ) as conteudo
        from {{ nc2_fonte("nota_carioca", "NOTAS_NACIONAIS") }}
        where nsu <= {{ nc2_corte() }}
        union all
        select
            concat('EVENTO|', evento) as chave,
            to_json_string(
                struct(
                    chave_acesso as chave_acesso,
                    tipo_evento as tipo_evento,
                    data_evento as data_evento,
                    chave_acesso_substituta as chave_acesso_substituta
                )
            ) as conteudo
        from {{ nc2_fonte("nota_carioca", "EVENTOS_DFEN") }}
        where evento is not null and nsu <= {{ nc2_corte() }}
    ),

    comparado as (
        select
            o.chave as chave,
            v.chave is not null as no_raw,
            v.conteudo is not distinct from o.conteudo as conteudo_igual
        from v2 as v
        full outer join oracle as o on o.chave = v.chave
        where o.chave is not null
    )

select
    'R-05' as regra,
    chave,
    if(no_raw and conteudo_igual, 'igual', 'divergente') as classe,
    case
        when not no_raw
        then 'ausente_no_raw'
        when not conteudo_igual
        then 'conteudo_diferente'
    end as detalhe
from comparado
