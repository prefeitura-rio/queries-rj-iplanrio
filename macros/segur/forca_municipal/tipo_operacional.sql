-- Deriva o tipo_operacional a partir de sinais operacionais da feature.
-- Sinais: tipo_missao, tipo_geometria, st_area(geometry), roteiro_norm.
--
-- Tipos possíveis (classificação geométrica/operacional):
--   sede    : feature da pasta QMD sem missão associada (KML only)
--   patrulha: rota de patrulhamento PTR — LineString
--   posto   : ponto fixo PB — Point
--   area    : polígono de base inteira RF/SV/SP (> limite_area_m2)
--   subarea : polígono de zona operacional RF/SV/SP (≤ limite_area_m2)
--   NULL    : sem geometry (DS) ou combinação não mapeada
--
-- Parâmetros obrigatórios:
--   tipo_missao, tipo_geometria, geometry, roteiro_norm
--
-- Parâmetros opcionais:
--   kml_folder, id_missao : ativa o branch 'sede' — passar apenas no KML
--   limite_area_m2        : limiar em m² entre area e subarea (default: 10 km²)
{% macro tipo_operacional(
    tipo_missao,
    tipo_geometria,
    geometry,
    roteiro_norm,
    limite_area_m2=10000000,
    kml_folder=none,
    id_missao=none
) %}
case
    {%- if kml_folder is not none and id_missao is not none %}
    -- Sede QMD: pasta qmd + missão nula (dois sinais independentes)
    when lower(trim({{ kml_folder }})) = 'qmd' and {{ id_missao }} is null
        then 'sede'
    {%- endif %}
    -- Patrulha PTR: rota de patrulhamento — LineString
    when
        lower(trim({{ tipo_missao }})) = 'ptr'
        and lower(trim({{ tipo_geometria }})) = 'linestring'
        then 'patrulha'
    -- Posto fixo PB: ponto de base — Point
    when
        lower(trim({{ tipo_missao }})) = 'pb'
        and lower(trim({{ tipo_geometria }})) = 'point'
        then 'posto'
    -- Área de base inteira: RF/SV/SP Polygon > limite OU 'area da base' no roteiro
    when
        lower(trim({{ tipo_missao }})) in ('rf', 'sv', 'sp')
        and lower(trim({{ tipo_geometria }})) = 'polygon'
        and (
            st_area({{ geometry }}) > {{ limite_area_m2 }}
            or regexp_contains({{ roteiro_norm }}, r'area da base')
        )
        then 'area'
    -- Subárea operacional: RF/SV/SP Polygon ≤ limite OU 'dentro da subarea' no roteiro
    when
        lower(trim({{ tipo_missao }})) in ('rf', 'sv', 'sp')
        and lower(trim({{ tipo_geometria }})) = 'polygon'
        and (
            st_area({{ geometry }}) <= {{ limite_area_m2 }}
            or regexp_contains({{ roteiro_norm }}, r'dentro da subarea')
        )
        then 'subarea'
end
{%- endmacro %}
