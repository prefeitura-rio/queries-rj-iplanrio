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
-- Geometria da missão incluída diretamente (geometria_wkt + geometry).
with
    qmd_servicos as (
        select id_servico, id_plano, id_qmd, id_unidade, tipo_unidade, base_operacional
        from {{ ref("raw_segur_forca_municipal__qmd_servicos") }}
    ),

    qmd_plano as (
        select
            id_plano,
            nome,
            area,
            numero_semana,
            data_semana_referencia_inicio,
            data_semana_referencia_fim,
            indicador_plano_unificado,
            indicador_plano_encerrado,
            indicador_plano_teste
        from {{ ref("raw_segur_forca_municipal__qmd_plano") }}
    ),

    -- QUALIFY: qmd é SCD — múltiplos snapshots por id_qmd quando indicadores mudam.
    -- Seleciona o snapshot mais recente para evitar fan-out no join.
    qmd as (
        select
            id_qmd,
            nome,
            localizacao_patrulha,
            data_hora_vigencia_inicio,
            data_hora_vigencia_fim,
            hora_inicio_qmd,
            hora_fim_qmd,
            indicador_ativo,
            indicador_valido,
            indicador_autorizado,
            indicador_hora_cruza_meia_noite,
            duracao_minutos_qmd,
            prescricoes,
            resumo
        from {{ ref("raw_segur_forca_municipal__qmd") }}
        qualify row_number() over (partition by id_qmd order by last_seen desc) = 1
    ),

    qmd_missoes as (
        select
            id_servico,
            id_qmd,
            id_missao,
            tipo_missao,
            tipo_missao_nome,
            tipo_operacional,
            tipo_geometria,
            roteiro,
            id_roteiro,
            id_subarea,
            id_area,
            hora_inicio_missao,
            hora_fim_missao,
            dias,
            execucoes,
            geometria_wkt,
            geometry
        from {{ ref("raw_segur_forca_municipal__qmd_missoes") }}
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
            miss.tipo_missao_nome,
            miss.tipo_operacional,
            miss.tipo_geometria,
            miss.roteiro,
            miss.id_roteiro,
            miss.id_subarea,
            miss.id_area,
            miss.hora_inicio_missao,
            miss.hora_fim_missao,
            miss.dias,
            miss.execucoes,
            miss.geometria_wkt,
            miss.geometry

        from qmd_servicos as srv
        inner join qmd_plano as plano on srv.id_plano = plano.id_plano
        inner join qmd on srv.id_qmd = qmd.id_qmd
        inner join qmd_missoes as miss
            on  srv.id_servico = miss.id_servico
            and srv.id_qmd     = miss.id_qmd
    )

select
    -- identificadores
    id_plano,
    id_qmd,
    id_servico,
    id_missao,
    id_unidade,

    -- plano semanal
    nome_plano,
    area,
    numero_semana,
    data_semana_referencia_inicio,
    data_semana_referencia_fim,
    indicador_plano_unificado,
    indicador_plano_encerrado,
    indicador_plano_teste,

    -- QMD
    nome_qmd,
    localizacao_patrulha,
    data_hora_vigencia_inicio,
    data_hora_vigencia_fim,
    hora_inicio_qmd,
    hora_fim_qmd,
    indicador_ativo,
    indicador_valido,
    indicador_autorizado,
    indicador_hora_cruza_meia_noite,
    duracao_minutos_qmd,
    prescricoes,
    resumo,

    -- unidade
    tipo_unidade,
    base_operacional,

    -- missão
    tipo_missao,
    tipo_missao_nome,
    tipo_operacional,
    tipo_geometria,
    roteiro,
    id_roteiro,
    id_subarea,
    id_area,
    hora_inicio_missao,
    hora_fim_missao,
    dias,
    execucoes,
    geometria_wkt,
    geometry

from joined