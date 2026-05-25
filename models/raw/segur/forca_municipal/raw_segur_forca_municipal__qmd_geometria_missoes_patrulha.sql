{{
    config(
        alias="qmd_geometria_missoes_patrulha",
        schema="brutos_forca_municipal",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id_hash",
        merge_update_columns=["last_seen", "data_particao", "updated_at", "id_subarea", "id_area"],
        cluster_by=["id_qmd"],
    )
}}

-- Rotas de patrulhamento (PTR) — geometria LineString.
-- Derivado de qmd_geometria_kml. Filtro: tipo_operacional = 'patrulha'.
-- Join canônico: qmd_missoes LEFT JOIN qmd_geometria_missoes_patrulha USING (id_missao)
select
    id_hash,
    id_hash_original,
    first_seen,
    last_seen,
    updated_at,
    data_particao,
    id_qmd,
    id_missao,
    nome,
    tipo_missao,
    tipo_missao_nome,
    tipo_geometria,
    tipo_operacional,
    hora_inicio_missao,
    hora_fim_missao,
    roteiro,
    id_roteiro,
    id_subarea,
    id_area,
    servicos,
    descricao,
    dados_extendidos,
    geometria_wkt,
    geometry
from {{ ref("raw_segur_forca_municipal__qmd_geometria_kml") }}
where tipo_operacional = 'patrulha'