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
-- Derivado de qmd_kml. Responsabilidade: representar zonas de atuação planejadas.
-- indicador_geometry_util = FALSE para polígonos de base inteira sem valor analítico
-- (SV, SP e RF genérica "Dentro da área da Base X").
-- Join canônico: qmd_missoes LEFT JOIN qmd_geometria_missoes_areas USING (id_missao)

select
    id_hash, first_seen, last_seen, updated_at, data_particao,
    id_qmd, id_missao,
    nome, tipo_missao, tipo_geometria,
    hora_inicio_missao, hora_fim_missao,
    roteiro, servicos,
    descricao, dados_extendidos,
    geometria_wkt, geometry,
    -- Sinaliza se o polígono tem valor analítico para conformidade.
    -- FALSE para missões cujo polígono é a área da base inteira:
    --   SV e SP: usam o polígono da base como geometry.
    --   RF sem subárea: roteiro genérico "Dentro da área da Base X".
    --     Critério primário: presença de "subárea" no roteiro (semântico).
    --     Critério secundário: ST_AREA < 50 km² (safety net para roteiros mal cadastrados).
    (
        tipo_missao not in ('SV', 'SP')
        and (
            tipo_missao != 'RF'
            or regexp_contains(lower(roteiro), r'sub.?rea')
            or st_area(geometry) / 1e6 < 50
        )
    ) as indicador_geometry_util
from {{ ref('raw_segur_forca_municipal__qmd_geometria_kml') }}
where tipo_missao in ('RF', 'SV', 'SP')
