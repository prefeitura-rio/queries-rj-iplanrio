{{
    config(
        alias="ocorrencias",
        schema="forca_municipal",
        materialized="table",
        cluster_by=["tipo_ocorrencia_codigo", "area_planejamento"],
    )
}}

-- Uma linha por ocorrência — estado final de ocorrencias_historico (indicador_ultima_revisao).
-- Localização e métricas de despacho (TTD / TTER / TTOA) via ocorrencias_ativas_v2.
-- NULL nesses campos indica ocorrências fechadas antes de serem capturadas pelo feed ativas.
--
-- P1: WHERE NOT indicador_evento_operacional
-- P4: ttd_segundos / tter_segundos / ttoa_segundos

with
    historico as (
        select *
        from {{ ref("raw_segur_forca_municipal__ocorrencias_historico") }}
        where indicador_ultima_revisao
    ),

    -- Observação mais recente por ocorrência em ativas_v2.
    -- Fornece localização e timeline de despacho — campos ausentes em ocorrencias_historico.
    ativas as (
        select
            id_ocorrencia,
            id_unidade_primaria,
            latitude,
            longitude,
            geometry,
            data_hora_primeiro_despacho,
            data_hora_primeira_unidade_a_caminho,
            data_hora_primeira_chegada,
            ttd_segundos,
            tter_segundos,
            ttoa_segundos
        from {{ ref("raw_segur_forca_municipal__ocorrencias_ativas_v2") }}
        qualify row_number() over (
            partition by id_ocorrencia order by last_seen desc
        ) = 1
    )

select
    -- identificadores
    hist.id_ocorrencia,
    hist.id_agencia,
    hist.id_esz,

    -- classificação
    hist.tipo_ocorrencia_codigo,
    hist.tipo_ocorrencia_descricao,
    hist.subtipo_ocorrencia_codigo,
    hist.subtipo_ocorrencia_descricao,
    hist.descricao_ocorrencia,
    hist.prioridade,
    hist.indicador_desvio_missao,
    hist.indicador_evento_operacional,

    -- temporal da ocorrência
    hist.data_hora_criacao,
    hist.data_hora_inicio,
    hist.data_hora_fechamento,
    hist.duracao_ocorrencia_minutos,

    -- temporal de despacho — NULL quando não capturado em ativas
    ativ.data_hora_primeiro_despacho,
    ativ.data_hora_primeira_unidade_a_caminho,
    ativ.data_hora_primeira_chegada,

    -- métricas de resposta — NULL quando timestamps ausentes
    ativ.ttd_segundos,
    ativ.tter_segundos,
    ativ.ttoa_segundos,

    -- estado
    hist.indicador_aberta,
    hist.indicador_reaberta,
    hist.total_unidades_atribuidas,
    hist.numero_revisao            as numero_ultima_revisao,
    hist.comentario_fechamento,

    -- unidade
    ativ.id_unidade_primaria,

    -- geográfico (textual)
    hist.area_planejamento,
    hist.setor,
    hist.distrito,
    hist.municipio,
    hist.zona,
    hist.grupo_despacho,

    -- geográfico (espacial) — NULL para ocorrências não capturadas em ativas
    ativ.latitude,
    ativ.longitude,
    ativ.geometry

from historico as hist
left join ativas as ativ on hist.id_ocorrencia = ativ.id_ocorrencia
