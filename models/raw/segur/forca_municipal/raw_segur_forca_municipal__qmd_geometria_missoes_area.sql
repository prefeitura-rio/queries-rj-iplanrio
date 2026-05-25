{{
    config(
        alias="qmd_geometria_missoes_area",
        schema="brutos_forca_municipal",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id_hash",
        merge_update_columns=["last_seen", "data_particao", "updated_at", "id_subarea", "id_area"],
        cluster_by=["id_qmd"],
    )
}}

-- Polígonos de base inteira RF/SV/SP com ST_AREA > 10 km² — geometria Polygon.
-- Derivado de qmd_missoes. Filtro: tipo_operacional = 'area'.
-- Grain: (id_missao, id_servico) — um polígono por plano semanal.
-- Inclui apenas missões com unidade alocada. Para o mapa completo com missões
-- não alocadas, usar qmd_geometria_kml (filtro tipo_operacional = 'area').
select
    id_hash,
    first_seen,
    last_seen,
    updated_at,
    data_particao,
    id_hash_pai,
    id_qmd,
    id_unidade,
    id_missao,
    id_servico,
    tipo_missao,
    roteiro_raw,
    hora_inicio_missao,
    hora_fim_missao,
    dias,
    execucoes,
    indicador_ativo,
    tipo_geometria,
    geometria_wkt,
    geometry,
    roteiro,
    tipo_missao_nome,
    tipo_operacional,
    id_roteiro,
    id_subarea,
    id_area
from {{ ref("raw_segur_forca_municipal__qmd_missoes") }}
where tipo_operacional = 'area'
{% if is_incremental() %}
    and data_particao >= (select max(data_particao) from {{ this }})
{% endif %}
