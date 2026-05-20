{{
    config(
        alias="qmd_missoes",
        schema="forca_municipal",
        materialized="table",
        cluster_by=["id_qmd", "id_unidade"],
    )
}}

-- Mart canônico de planejamento operacional da Força Municipal.
-- Grain: uma linha por unidade × missão (id_servico × id_missao).
-- Elimina o join de 5 tabelas que toda query downstream precisava re-escrever.
-- Geometrias disponíveis nas tabelas raw qmd_geometria_missoes_* e qmd_geometria_bases.
with
    qmd_servicos as (
        select * from {{ ref("raw_segur_forca_municipal__qmd_servicos") }}
    ),

    qmd_plano as (select * from {{ ref("raw_segur_forca_municipal__qmd_plano") }}),

    qmd as (select * from {{ ref("raw_segur_forca_municipal__qmd") }}),

    qmd_missoes as (select * from {{ ref("raw_segur_forca_municipal__qmd_missoes") }}),

    joined as (
        select
            -- identificadores
            plano.id_plano,
            qmd.id_qmd,
            srv.id_servico,
            miss.id_missao,
            srv.id_unidade,

            -- plano semanal
            plano.nome as nome_plano,
            plano.area,
            plano.numero_semana,
            plano.data_semana_referencia_inicio,
            plano.data_semana_referencia_fim,
            plano.indicador_plano_unificado,
            plano.indicador_plano_encerrado,
            plano.indicador_plano_teste,

            -- QMD
            qmd.nome as nome_qmd,
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
            miss.tipo_geometria,
            miss.roteiro,
            miss.hora_inicio_missao,
            miss.hora_fim_missao,
            miss.dias,
            miss.execucoes

        from qmd_servicos as srv
        inner join qmd_plano as plano on srv.id_plano = plano.id_plano
        inner join qmd on srv.id_qmd = qmd.id_qmd
        inner join qmd_missoes as miss on srv.id_servico = miss.id_servico
    )

select *
from joined