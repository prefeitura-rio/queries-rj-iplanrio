{{
    config(
        alias="qmd_geometria_sede",
        schema="brutos_forca_municipal",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id_hash",
        merge_update_columns=["last_seen", "data_particao", "updated_at"],
        cluster_by=["id_qmd"],
    )
}}

-- Derivado de qmd_geometria_kml. Filtro: tipo_operacional = 'sede'.
select
    id_hash,
    first_seen,
    last_seen,
    updated_at,
    data_particao,
    id_qmd,
    nome,
    tipo_geometria,
    tipo_operacional,
    descricao,
    geometria_wkt,
    geometry
from {{ ref("raw_segur_forca_municipal__qmd_geometria_kml") }}
where tipo_operacional = 'sede'