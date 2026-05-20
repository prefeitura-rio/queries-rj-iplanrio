{{
    config(
        alias="qmd_geometria_missoes_areas",
        schema="brutos_forca_municipal",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id_hash",
        merge_update_columns=["last_seen", "data_particao", "updated_at"],
        cluster_by=["id_qmd"],
    )
}}

-- Áreas de cobertura (RF, SV, SP) — geometria Polygon.
-- Derivado de qmd_geometria_kml. Filtro: tipo_missao IN ('RF', 'SV', 'SP').
-- indicador_geometry_util e tipo_area computados no base model.
-- Join canônico: qmd_missoes LEFT JOIN qmd_geometria_missoes_areas USING (id_missao)

select
    id_hash, first_seen, last_seen, updated_at, data_particao,
    id_qmd, id_missao,
    nome, tipo_missao, tipo_geometria, tipo_area,
    hora_inicio_missao, hora_fim_missao,
    roteiro, servicos,
    indicador_geometry_util,
    descricao, dados_extendidos,
    geometria_wkt, geometry
from {{ ref('raw_segur_forca_municipal__qmd_geometria_kml') }}
where tipo_missao in ('RF', 'SV', 'SP')
