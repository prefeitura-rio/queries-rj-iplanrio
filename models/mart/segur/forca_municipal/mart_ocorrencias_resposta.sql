{{
    config(
        alias="mart_ocorrencias_resposta",
        schema="forca_municipal",
        materialized="table",
        cluster_by=["tipo_ocorrencia_codigo", "area_planejamento"],
    )
}}

-- Mart de resposta a ocorrências da Força Municipal (P4).
-- Grain: uma linha por id_ocorrencia (revisão mais recente).
--
-- Resolve dois gaps que nenhum modelo raw resolve isolado:
--   1. TTD histórico: ocorrencias_historico tem duração mas não tem timestamps de
--      despacho; ocorrencias_ativas_v2 tem os timestamps mas é incremental.
--      O join por id_ocorrencia une as duas fontes.
--
--   2. Quais unidades responderam: ocorrencias_historico tem apenas
--      total_unidades_atribuidas (inteiro da API). unidades_historico tem
--      id_ocorrencia_atribuida por evento de status — permite reconstruir o
--      conjunto exato de unidades que atuaram em cada ocorrência.
--
-- Cobertura de TTD: apenas ocorrências que apareceram em ocorrencias_ativas_v2.
-- Ocorrências anteriores ao go-live desse endpoint terão ttd_segundos = NULL.
-- unidades_atribuidas cobre todo o histórico via unidades_historico.

with
    -- Uma linha por ocorrência: última revisão disponível.
    hist as (
        select *
        from {{ ref("raw_segur_forca_municipal__ocorrencias_historico") }}
        where indicador_ultima_revisao = true
    ),

    -- Uma linha por ocorrência em ocorrencias_ativas_v2.
    -- Prefere o registro com data_hora_primeiro_despacho preenchido;
    -- em caso de empate, o mais recente por data_hora_atualizacao_bd (timestamp de negócio da API).
    ativas as (
        select *
        from {{ ref("raw_segur_forca_municipal__ocorrencias_ativas_v2") }}
        qualify row_number() over (
            partition by id_ocorrencia
            order by
                (data_hora_primeiro_despacho is not null) desc,
                data_hora_atualizacao_bd desc
        ) = 1
    ),

    -- Unidades que atuaram em cada ocorrência, reconstruídas a partir dos
    -- eventos de status em unidades_historico.
    -- total_unidades_confirmadas pode diferir de total_unidades_atribuidas (API)
    -- quando unidades foram associadas sem gerar evento de status registrado.
    unidades_por_ocorrencia as (
        select
            id_ocorrencia_atribuida                                         as id_ocorrencia,
            array_agg(
                distinct id_unidade ignore nulls order by id_unidade
            )                                                               as unidades_atribuidas,
            count(distinct id_unidade)                                      as total_unidades_confirmadas
        from {{ ref("raw_segur_forca_municipal__unidades_historico") }}
        where id_ocorrencia_atribuida is not null
        group by id_ocorrencia_atribuida
    ),

    joined as (
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
            hist.nivel_alarme,
            hist.id_status,
            hist.indicador_aberta,
            hist.indicador_reaberta,
            hist.indicador_desvio_missao,
            hist.indicador_evento_operacional,

            -- timestamps da ocorrência (fonte: historico)
            hist.data_hora_criacao,
            hist.data_hora_inicio,
            hist.data_hora_fechamento,
            hist.duracao_ocorrencia_minutos,
            hist.comentario_fechamento,

            -- timestamps de resposta (fonte: ativas_v2; NULL se fora da cobertura)
            ativas.data_hora_primeiro_despacho,
            ativas.data_hora_primeira_unidade_a_caminho,
            ativas.data_hora_primeira_chegada,

            -- métricas de tempo de resposta em segundos
            -- TTD  = criação → primeiro despacho
            -- TTER = primeiro despacho → primeira unidade a caminho
            -- TTOA = primeiro despacho → primeira chegada
            ativas.ttd_segundos,
            ativas.tter_segundos,
            ativas.ttoa_segundos,

            -- unidades
            ativas.id_unidade_primaria,
            hist.total_unidades_atribuidas,
            coalesce(unid.total_unidades_confirmadas, 0)    as total_unidades_confirmadas,
            coalesce(unid.unidades_atribuidas, [])          as unidades_atribuidas,

            -- geografia
            hist.area_planejamento,
            hist.setor,
            hist.distrito,
            hist.municipio,
            hist.zona,
            hist.grupo_despacho,
            ativas.localizacao,

            -- espacial (fonte: ativas_v2; ocorrencias_historico não tem geometry)
            ativas.latitude,
            ativas.longitude,
            ativas.geometry

        from hist
        left join ativas       using (id_ocorrencia)
        left join unidades_por_ocorrencia as unid using (id_ocorrencia)
    )

select *
from joined
