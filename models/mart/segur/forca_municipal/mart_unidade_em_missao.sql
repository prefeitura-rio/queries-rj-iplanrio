{{
    config(
        alias="mart_unidade_em_missao",
        schema="forca_municipal",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
        cluster_by=["id_unidade", "id_qmd"],
    )
}}

-- Mart de conformidade operacional P3 da Força Municipal.
-- Grain: uma linha por ponto GPS × execução planejada de missão.
--
-- Tolerância para missões lineares (PTR) e pontuais (PB):
--   GPS é considerado conforme se ST_DWITHIN(gps, geometry, TOLERANCIA_METROS).
--   Ajuste este valor conforme critério operacional do CompStat.
{% set TOLERANCIA_METROS = 30 %}

-- Para cada leitura GPS de unit_positions, determina:
--   estado_conformidade = 'CONFORME' → dentro da área/rota planejada
--   estado_conformidade = 'DESPACHO' → fora da área, mas em despacho (justificado)
--   estado_conformidade = 'DESVIO'   → fora da área sem despacho (alerta P3)
--
-- Lógica de conformidade por tipo de geometria:
--   POLYGON    (RF, SV, SP) → ST_WITHIN(gps, poligono)
--   LINESTRING (PTR)        → ST_DWITHIN(gps, rota, TOLERANCIA_METROS)
--   POINT      (PB)         → ST_DWITHIN(gps, ponto, TOLERANCIA_METROS)
--
-- Janela temporal: join em execucoes[].data_hora_inicio/fim — a fonte
-- autoritativa por execução. Substitui hora+dia_semana+meia_noite.
--
-- unidades_historico contém o histórico completo desde o go-live em dump
-- cumulativo — cobertura total para qualquer GPS histórico.

with
    gps as (
        select *
        from {{ ref("raw_segur_forca_municipal__unit_positions") }}
        {% if is_incremental() %}
            where data_particao >= (
                select max(data_particao) from {{ this }}
            )
        {% endif %}
    ),

    -- Missões ativas com geometry definida.
    -- DS (deslocamento) excluído — sem geometry no KML.
    missoes as (
        select *
        from {{ ref("mart_qmd_missoes_completo") }}
        where indicador_ativo = true
          and geometry_missao is not null
    ),

    -- Converte eventos de status em intervalos [inicio, fim).
    status_periodos as (
        select
            id_unidade,
            data_hora_criacao                                           as inicio,
            lead(data_hora_criacao) over (
                partition by id_unidade order by data_hora_criacao
            )                                                           as fim,
            indicador_despachada
        from {{ ref("raw_segur_forca_municipal__unidades_historico") }}
    ),

    -- Join principal: GPS × execução planejada.
    -- O unnest de execucoes expande cada missão em N linhas (uma por execução),
    -- e a condição BETWEEN filtra apenas a execução cujo intervalo contém o GPS.
    -- Resulta em no máximo uma execução por (GPS × missão).
    gps_por_execucao as (
        select
            -- rastreabilidade
            gps.id_hash,
            gps.data_particao,
            gps.data_hora,
            gps.data_coleta,

            -- identificadores
            gps.id_unidade,
            miss.id_qmd,
            miss.id_plano,
            miss.id_servico,
            miss.id_missao,

            -- contexto do plano / QMD
            miss.area,
            miss.numero_semana,
            miss.data_semana_referencia_inicio,
            miss.data_semana_referencia_fim,
            miss.nome_qmd,
            miss.tipo_unidade,
            miss.base_operacional,
            miss.hora_inicio_qmd,
            miss.hora_fim_qmd,
            miss.indicador_hora_cruza_meia_noite,
            miss.duracao_minutos_qmd,

            -- contexto da missão
            miss.tipo_missao,
            miss.roteiro,
            exec.data_hora_inicio                                  as data_hora_inicio_exec,
            exec.data_hora_fim                                     as data_hora_fim_exec,
            exec.week_day                                          as dia_semana,
            exec.week_day_number                                   as dia_semana_numero,
            miss.tipo_geometria,

            -- conformidade espacial por tipo de geometria
            case miss.tipo_geometria
                when 'POLYGON'
                    then st_within(gps.geometry, miss.geometry_missao)
                else
                    -- LINESTRING (PTR) e POINT (PB): tolerância em metros
                    st_dwithin(gps.geometry, miss.geometry_missao, {{ TOLERANCIA_METROS }})
            end                                                    as indicador_conforme,

            -- posição GPS
            gps.latitude,
            gps.longitude,
            gps.geometry                                           as geometry_gps,
            miss.geometry_missao

        from gps
        inner join missoes as miss
            on miss.id_unidade = gps.id_unidade
            -- vigência do QMD (data): GPS dentro do período do plano
            and gps.data_hora >= miss.data_hora_vigencia_inicio
            and gps.data_hora <  miss.data_hora_vigencia_fim,
        -- unnest das execuções planejadas: filtro por janela exata do dia
        unnest(miss.execucoes) as exec
        where gps.data_hora between exec.data_hora_inicio and exec.data_hora_fim
    ),

    -- Adiciona status de despacho. QUALIFY garante uma linha por
    -- (id_hash, id_missao) em caso de intervalos sobrepostos em status_periodos.
    com_despacho as (
        select
            gps_por_execucao.*,
            coalesce(st.indicador_despachada, false) as indicador_em_despacho
        from gps_por_execucao
        left join status_periodos as st
            on  st.id_unidade       = gps_por_execucao.id_unidade
            and gps_por_execucao.data_hora >= st.inicio
            and (gps_por_execucao.data_hora < st.fim or st.fim is null)
        qualify row_number() over (
            partition by gps_por_execucao.id_hash, gps_por_execucao.id_missao
            order by coalesce(st.inicio, datetime '1970-01-01') desc
        ) = 1
    ),

    com_estado as (
        select
            *,
            case
                when indicador_conforme       then 'CONFORME'
                when indicador_em_despacho    then 'DESPACHO'
                else                               'DESVIO'
            end as estado_conformidade
        from com_despacho
    )

select *
from com_estado
