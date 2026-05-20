{{
    config(
        alias="qmd_geometria_bases",
        schema="brutos_forca_municipal",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id_hash",
        merge_update_columns=["last_seen", "data_particao", "updated_at"],
        cluster_by=["id_qmd"],
    )
}}

-- Derivado de qmd_kml. Filtra pela pasta 'QMD' (localização das bases operacionais).

select
    id_hash, first_seen, last_seen, updated_at, data_particao,
    id_qmd,
    nome, tipo_geometria,
    descricao,
    geometria_wkt, geometry
from {{ ref("raw_segur_forca_municipal__qmd_geometria_kml") }}
where kml_folder in ('qmd', 'qmds')