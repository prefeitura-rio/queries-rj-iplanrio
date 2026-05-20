{{
    config(
        alias="qmd_geometria_missoes_pontos",
        schema="brutos_forca_municipal",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id_hash",
        merge_update_columns=["last_seen", "data_particao", "updated_at"],
        cluster_by=["id_qmd"],
    )
}}

-- Pontos de bloqueio (PB) — geometria Point.
-- Derivado de qmd_kml. Responsabilidade: representar posições fixas de bloqueio.
-- Join canônico: qmd_missoes LEFT JOIN qmd_geometria_missoes_pontos USING (id_missao)

select
    id_hash, first_seen, last_seen, updated_at, data_particao,
    id_qmd, id_missao,
    nome, tipo_missao, tipo_geometria,
    hora_inicio_missao, hora_fim_missao,
    roteiro, servicos,
    descricao, dados_extendidos,
    geometria_wkt, geometry
from {{ ref('raw_segur_forca_municipal__qmd_geometria_kml') }}
where tipo_missao = 'PB'
