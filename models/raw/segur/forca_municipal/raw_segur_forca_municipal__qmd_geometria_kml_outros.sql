{{
    config(
        alias="qmd_geometria_kml_outros",
        schema="brutos_forca_municipal",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id_hash",
        merge_update_columns=["last_seen", "data_particao", "updated_at"],
        cluster_by=["id_qmd"],
    )
}}

-- Catch-all: captura qualquer kml_folder não reconhecido pelos modelos derivados.
-- Derivado de qmd_kml. Em condições normais esta tabela deve ter zero linhas.
-- total_linhas > 0 indica que a API introduziu um novo tipo de pasta KML
-- — investigar e criar modelo dedicado.

select
    id_hash, first_seen, last_seen, updated_at, data_particao,
    id_qmd,
    kml_folder_raw,
    nome, tipo_geometria,
    descricao, dados_extendidos,
    geometria_wkt
from {{ ref('raw_segur_forca_municipal__qmd_geometria_kml') }}
where kml_folder not in ('qmd', 'qmds', 'missoes', 'missao')
