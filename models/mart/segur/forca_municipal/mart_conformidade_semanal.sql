{{
    config(
        alias="mart_conformidade_semanal",
        schema="forca_municipal",
        materialized="table",
        cluster_by=["area", "id_semana"],
    )
}}

-- Mart de conformidade operacional semanal para o comitê CompStat (P3).
-- Grain: uma linha por semana × área × missão.
--
-- id_semana: sempre o domingo que inicia a semana (DATE_TRUNC WEEK(SUNDAY)).
-- Percentuais de conforme + despacho + desvio somam 100% por linha.
-- Fonte: mart_unidade_em_missao (GPS × missão × estado de conformidade).
--
-- Missões com geometry inútil (SV, SP, RF de base) já foram excluídas em
-- mart_qmd_missoes_completo via filtro em qmd_missoes_geometria.indicador_geometry_util.
-- Não há regra de negócio duplicada aqui.

with
    base as (
        select *
        from {{ ref("mart_unidade_em_missao") }}
    ),

    agregado as (
        select
            -- identificador da semana (domingo de início)
            date_trunc(data_semana_referencia_inicio, week(sunday))  as id_semana,

            -- período da semana operacional
            data_semana_referencia_inicio,
            data_semana_referencia_fim,
            numero_semana,

            -- área e missão
            area,
            id_qmd,
            nome_qmd,
            id_missao,
            tipo_missao,
            roteiro,

            -- cobertura
            count(distinct id_unidade)                               as total_unidades,
            count(*)                                                 as total_pontos_gps,

            -- contagens por estado
            countif(estado_conformidade = 'CONFORME')                as total_conforme,
            countif(estado_conformidade = 'DESPACHO')                as total_despacho,
            countif(estado_conformidade = 'DESVIO')                  as total_desvio,

            -- percentuais (soma = 100% por linha)
            round(countif(estado_conformidade = 'CONFORME') * 100.0 / count(*), 1) as pct_conforme,
            round(countif(estado_conformidade = 'DESPACHO') * 100.0 / count(*), 1) as pct_despacho,
            round(countif(estado_conformidade = 'DESVIO')   * 100.0 / count(*), 1) as pct_desvio,

            -- geometria da missão para visualização
            any_value(geometry_missao)                               as geometry_missao

        from base
        group by
            data_semana_referencia_inicio,
            data_semana_referencia_fim,
            numero_semana,
            area,
            id_qmd,
            nome_qmd,
            id_missao,
            tipo_missao,
            roteiro
    )

select *
from agregado
