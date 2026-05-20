{{
    config(
        alias="turnos",
        schema="forca_municipal",
        materialized="table",
        cluster_by=["base_operacional", "tipo_unidade"],
    )
}}

-- Um turno por id_unidade × data_logon, consolidado a partir de unidades_historico.
-- Os contadores (tempo_total_*, total_ocorrencias) são acumuladores crescentes no sistema;
-- este modelo extrai o valor final do último evento do turno.
-- data_logon é a data do início do logon — turnos noturnos cruzam meia-noite.

with
    historico as (
        select * from {{ ref("raw_segur_forca_municipal__unidades_historico") }}
    ),

    -- Último evento de cada turno: carrega os contadores acumulados finais.
    ultimo_evento as (
        select
            id_unidade,
            data_logon,
            tipo_unidade,
            base_operacional,
            id_agencia,
            id_estacao,
            data_hora_logon,
            tempo_total_ocorrencias,
            tempo_total_indisponivel,
            tempo_total_disponivel_estacao,
            total_ocorrencias,
            data_hora_criacao as data_hora_ultimo_evento
        from historico
        qualify row_number() over (
            partition by id_unidade, data_logon
            order by data_hora_criacao desc, ordem_criacao desc
        ) = 1
    ),

    -- Métricas agregadas do turno.
    metricas as (
        select
            id_unidade,
            data_logon,
            min(data_hora_criacao)        as data_hora_primeiro_evento,
            count(*)                      as total_eventos,
            countif(indicador_despachada) as total_eventos_despachada
        from historico
        group by id_unidade, data_logon
    )

select
    -- identificadores
    u.id_unidade,
    u.data_logon,
    u.id_agencia,
    u.id_estacao,

    -- atributos da unidade
    u.tipo_unidade,
    u.base_operacional,

    -- temporal do turno
    u.data_hora_logon,
    m.data_hora_primeiro_evento,
    u.data_hora_ultimo_evento,

    -- contadores acumulados (valor final do turno)
    u.total_ocorrencias,
    u.tempo_total_ocorrencias,
    u.tempo_total_indisponivel,
    u.tempo_total_disponivel_estacao,

    -- métricas derivadas
    m.total_eventos,
    m.total_eventos_despachada

from ultimo_evento as u
inner join metricas as m
    on u.id_unidade = m.id_unidade
    and u.data_logon = m.data_logon
