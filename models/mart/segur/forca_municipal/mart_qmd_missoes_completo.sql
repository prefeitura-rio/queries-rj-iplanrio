{{
    config(
        alias="mart_qmd_missoes_completo",
        schema="forca_municipal",
        materialized="table",
        cluster_by=["id_qmd", "id_unidade"],
    )
}}

-- Mart canônico de planejamento operacional da Força Municipal.
-- Grain: uma linha por unidade × missão (id_servico × id_missao).
-- Elimina o join de 5 tabelas que toda query CompStat precisava re-escrever.
--
-- geometry_missao — polígono/linha/ponto da missão (fontes: qmd_geometria_missoes_*; NULL para DS)
-- geometry_qmd    — ponto físico do QMD            (fonte: qmd_bases)

with
    qmd_servicos as (
        select * from {{ ref("raw_segur_forca_municipal__qmd_servicos") }}
    ),

    qmd_plano as (
        select * from {{ ref("raw_segur_forca_municipal__qmd_plano") }}
    ),

    qmd as (
        select * from {{ ref("raw_segur_forca_municipal__qmd") }}
    ),

    qmd_missoes as (
        select * from {{ ref("raw_segur_forca_municipal__qmd_missoes") }}
    ),

    -- Geometrias de missões: UNION das três tabelas por tipo de geometry.
    -- Áreas filtradas por indicador_geometry_util (exclui polígonos de base inteira).
    qmd_missoes_geometria as (
        select id_missao, geometry, tipo_geometria
        from {{ ref("raw_segur_forca_municipal__qmd_geometria_missoes_rotas") }}
        union all
        select id_missao, geometry, tipo_geometria
        from {{ ref("raw_segur_forca_municipal__qmd_geometria_missoes_areas") }}
        where indicador_geometry_util
        union all
        select id_missao, geometry, tipo_geometria
        from {{ ref("raw_segur_forca_municipal__qmd_geometria_missoes_pontos") }}
    ),

    -- Alguns QMDs têm múltiplas features KML em qmd_bases.
    -- Colapsamos aqui para evitar fan-out no join.
    qmd_bases_por_qmd as (
        select
            id_qmd,
            any_value(geometry) as geometry_qmd
        from {{ ref("raw_segur_forca_municipal__qmd_geometria_bases") }}
        group by id_qmd
    ),

    joined as (
        select
            -- identificadores
            plano.id_plano,
            qmd.id_qmd,
            srv.id_servico,
            miss.id_missao,
            srv.id_unidade,

            -- plano semanal
            plano.nome                            as nome_plano,
            plano.area,
            plano.numero_semana,
            plano.data_semana_referencia_inicio,
            plano.data_semana_referencia_fim,
            plano.indicador_plano_unificado,
            plano.indicador_plano_encerrado,
            plano.indicador_plano_teste,

            -- QMD
            qmd.nome                              as nome_qmd,
            qmd.localizacao_patrulha,
            qmd.data_hora_vigencia_inicio,
            qmd.data_hora_vigencia_fim,
            qmd.hora_inicio_qmd,
            qmd.hora_fim_qmd,
            qmd.indicador_ativo,
            qmd.indicador_valido,
            qmd.indicador_autorizado,
            qmd.indicador_hora_cruza_meia_noite,
            qmd.duracao_minutos_qmd,
            qmd.prescricoes,
            qmd.resumo,

            -- unidade
            srv.tipo_unidade,
            srv.base_operacional,

            -- missão
            miss.tipo_missao,
            miss.roteiro,
            miss.hora_inicio_missao,
            miss.hora_fim_missao,
            miss.dias,
            miss.execucoes,
            miss.tipo_geometria,

            -- espacial
            geo.geometry      as geometry_missao,
            bases.geometry_qmd

        from qmd_servicos as srv
        inner join qmd_plano as plano           on srv.id_plano   = plano.id_plano
        inner join qmd                          on srv.id_qmd     = qmd.id_qmd
        inner join qmd_missoes as miss          on srv.id_servico = miss.id_servico
        left join qmd_missoes_geometria as geo  on miss.id_missao = geo.id_missao
        left join qmd_bases_por_qmd as bases    on srv.id_qmd     = bases.id_qmd
    )

select *
from joined
