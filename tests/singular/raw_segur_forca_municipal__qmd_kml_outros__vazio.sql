-- Falha se a API introduziu um novo tipo de kml_folder não reconhecido por
-- qmd_bases ('qmds', 'qmd') ou qmd_missoes_geometria ('missoes', 'missao').
-- Em condições normais esta tabela deve ter zero linhas.
-- Se falhar: inspecionar kml_folder_raw para identificar o novo valor
-- e criar modelo dedicado.

select
    kml_folder_raw,
    count(*) as total_linhas,
    min(first_seen) as primeiro_avistamento
from {{ ref('raw_segur_forca_municipal__qmd_geometria_kml_outros') }}
group by kml_folder_raw
