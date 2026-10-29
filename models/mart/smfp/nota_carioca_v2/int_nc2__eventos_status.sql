{{ config(cluster_by=["chave_acesso"]) }}

{#
    R-08 (só os eventos): por CHAVE_ACESSO vale o evento MAIS RECENTE entre os que mudam o status, ordenado por DATA_EVENTO desc e
    NSU desc (RPE_NOTASNACIONAIS.pkb:553-582). Correção catalogada B-06: o VB (Eventos.vb:266,571-579) guarda o PRIMEIRO evento do
    lote (menor NSU); o dbt não repete isso.
      e101101, e105104, e305101, e105102  status 1 (cancelada), DATA_CANCELAMENTO = DATA_EVENTO
      e105102                             traz também CHAVE_ACESSO_SUBSTITUTA
      e305102 (bloqueio de ofício)        status 2
      e305103 (desbloqueio de ofício)     status 0
    Demais eventos (e101103, e105105, e202201...) não alteram o status. DATA_CANCELAMENTO só existe quando o evento mais recente
    é de cancelamento (o Oracle mantém o valor anterior em bloqueio e desbloqueio: efeito de estado que a paridade R-08 mostraria
    como divergência, não como correção). nsu_max_evento é o maior NSU entre os eventos relevantes da nota: a paridade usa para
    cortar o que o Oracle ainda não viu.
#}
with
    relevantes as (
        select chave_acesso, tipo_evento, data_evento, chave_acesso_substituta, nsu
        from {{ ref("int_nc2__dedup_eventos") }}
        where
            chave_acesso is not null
            and tipo_evento
            in ('e101101', 'e105104', 'e305101', 'e105102', 'e305102', 'e305103')
    ),

    ultimo as (
        select *
        from relevantes
        qualify
            row_number() over (
                partition by chave_acesso order by data_evento desc, nsu desc
            )
            = 1
    ),

    maximo as (
        select chave_acesso, max(nsu) as nsu_max_evento
        from relevantes
        group by chave_acesso
    )

select
    u.chave_acesso,
    u.tipo_evento,
    case
        when u.tipo_evento in ('e101101', 'e105104', 'e305101', 'e105102')
        then 1
        when u.tipo_evento = 'e305102'
        then 2
        when u.tipo_evento = 'e305103'
        then 0
    end as status_nota,
    if(
        u.tipo_evento in ('e101101', 'e105104', 'e305101', 'e105102'),
        u.data_evento,
        null
    ) as data_cancelamento,
    if(
        u.tipo_evento = 'e105102', u.chave_acesso_substituta, null
    ) as chave_acesso_substituta,
    u.nsu as nsu_evento,
    m.nsu_max_evento
from ultimo as u
inner join maximo as m on m.chave_acesso = u.chave_acesso
