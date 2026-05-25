{{
    config(
        alias="qmd_geometria_kml",
        schema="brutos_forca_municipal",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id_hash",
        merge_update_columns=["last_seen", "data_particao", "updated_at", "id_subarea", "id_area", "id_hash_original"],
        cluster_by=["id_qmd", "tipo_missao"],
    )
}}

{#
  limite_area_m2: limiar em m² que separa polígonos de base inteira (tipo_operacional='area')
  de zonas operacionais (tipo_operacional='subarea').
  Afeta tipo_operacional e indicador_geometry_util — alterar aqui muda os dois campos.
  Gap histórico confirmado em staging: nenhum polígono entre 1.7 km² e 295 km².
  Qualquer valor entre 2 km² e 294 km² produz o mesmo resultado. 10 km² por legibilidade.
#}
{% set limite_area_m2 = 10 * 1000 * 1000 %}

-- Base model: todas as features KML do QMD, todas as pastas.
-- Centraliza as transformações de parsing e geometry. Modelos derivados apenas filtram.
-- Pastas conhecidas: 'Missões' (missoes/missao) → missões; 'QMD' (qmd/qmds) → bases.
-- Modelos derivados:
-- qmd_geometria_missoes_patrulha (tipo_missao = 'PTR')
-- qmd_geometria_missoes_area     (tipo_operacional = 'area')
-- qmd_geometria_missoes_subarea  (tipo_operacional = 'subarea')
-- qmd_geometria_missoes_posto    (tipo_missao = 'PB')
-- qmd_geometria_sede           (kml_folder = 'qmd')
-- qmd_kml_outros               (kml_folder fora dos valores conhecidos)
with
    source as (
        select *
        from {{ source("brutos_forca_municipal_staging", "qmd_kml") }}
        {% if is_incremental() %}
            where
                safe_cast(data_particao as date) = (
                    select max(safe_cast(data_particao as date))
                    from {{ source("brutos_forca_municipal_staging", "qmd_kml") }}
                )
        {% endif %}
    ),

    renamed as (
        select
            -- metadados da pipeline
            {{ padronize_id("id_hash") }} as id_hash_original,
            safe_cast(updated_at as datetime) as updated_at,
            safe_cast(data_particao as date) as data_particao,

            -- identificadores
            {{ padronize_id("qmd_id") }} as id_qmd,
            {{
                padronize_id(
                    "json_value(safe_cast(extended_data as string), '$.MissaoId')"
                )
            }} as id_missao,

            -- pasta KML
            lower(regexp_replace(normalize(kml_folder, nfd), r'\pM', '')) as kml_folder,
            safe_cast(kml_folder as string) as kml_folder_raw,

            -- dados
            {{ proper_br("safe_cast(name as string)") }} as nome,
            json_value(safe_cast(extended_data as string), '$.Tipo') as tipo_missao,
            upper(trim(safe_cast(geometry_type as string))) as tipo_geometria,
            safe.parse_time(
                '%H:%M', json_value(safe_cast(extended_data as string), '$.HoraInicio')
            ) as hora_inicio_missao,
            safe.parse_time(
                '%H:%M', json_value(safe_cast(extended_data as string), '$.HoraFim')
            ) as hora_fim_missao,
            -- roteiro_raw: valor bruto de $.Roteiro, apenas trim. Usado como sinal
            -- de redundância textual na classificação de tipo_operacional.
            trim(
                json_value(safe_cast(extended_data as string), '$.Roteiro')
            ) as roteiro_raw,
            -- roteiro: prefixos operacionais e redundâncias removidos via macro.
            -- Ver macro padronize_roteiro para a lista completa de padrões stripados.
            {{
                padronize_roteiro(
                    "json_value(safe_cast(extended_data as string), '$.Roteiro')"
                )
            }} as roteiro,
            -- servicos: unidades alocadas à missão (CSV da API → ARRAY de valores
            -- distintos)
            array(
                select distinct u
                from
                    unnest(
                        if(
                            json_value(safe_cast(extended_data as string), '$.Servicos')
                            is null,
                            [],
                            split(
                                trim(
                                    json_value(
                                        safe_cast(extended_data as string), '$.Servicos'
                                    )
                                ),
                                ', '
                            )
                        )
                    ) as u
            ) as servicos,
            safe_cast(description as string) as descricao,
            safe_cast(extended_data as string) as dados_extendidos,

            -- espacial
            safe_cast(geometry_wkt as string) as geometria_wkt,
            safe.st_geogfromtext(
                safe_cast(geometry_wkt as string), make_valid => true
            ) as geometry

        from source
    ),

    -- -------------------------------------------------------------------------
    -- Hash estável: exclui descricao e servicos (campos voláteis).
    -- O id_hash_original do upstream muda semanalmente porque descricao acumula
    -- a lista de todos os planos que referenciam a geometry (cresce a cada novo
    -- id_plano). O id_hash novo é determinístico sobre campos estruturais
    -- imutáveis, garantindo 1 linha por geometry distinta no SCD.
    -- Campos incluídos: id_qmd, id_missao, geometria_wkt, tipo_missao,
    --   hora_inicio_missao, hora_fim_missao, roteiro_raw.
    -- -------------------------------------------------------------------------
    com_hash_estavel as (
        select
            to_hex(md5(concat(
                coalesce(id_qmd,        ''), '|',
                coalesce(id_missao,     ''), '|',
                coalesce(geometria_wkt, ''), '|',
                coalesce(tipo_missao,   ''), '|',
                coalesce(cast(hora_inicio_missao as string), ''), '|',
                coalesce(cast(hora_fim_missao    as string), ''), '|',
                coalesce(roteiro_raw,   '')
            ))) as id_hash,
            *
        from renamed
    ),

    deduplicado as (
        select
            -- metadados da pipeline
            id_hash,
            min(updated_at) as first_seen,
            max(updated_at) as last_seen,
            max(updated_at) as updated_at,
            max(data_particao) as data_particao,

            -- identificadores
            any_value(id_qmd) as id_qmd,
            any_value(id_missao) as id_missao,

            -- pasta KML
            any_value(kml_folder) as kml_folder,
            any_value(kml_folder_raw) as kml_folder_raw,

            -- dados
            any_value(nome) as nome,
            any_value(tipo_missao) as tipo_missao,
            any_value(tipo_geometria) as tipo_geometria,
            any_value(hora_inicio_missao) as hora_inicio_missao,
            any_value(hora_fim_missao) as hora_fim_missao,
            any_value(roteiro_raw) as roteiro_raw,
            any_value(roteiro) as roteiro,
            any_value(servicos) as servicos,
            any_value(descricao) as descricao,
            any_value(dados_extendidos) as dados_extendidos,

            -- espacial
            any_value(geometria_wkt) as geometria_wkt,
            any_value(geometry) as geometry,

            -- hash original do upstream (volátil — mantido para rastreabilidade)
            any_value(id_hash_original) as id_hash_original
        from com_hash_estavel
        group by id_hash
    ),

    enriquecido as (
        -- Normaliza roteiro_raw para uso nos sinais de redundância de tipo_operacional.
        -- Normalização: lower + trim + remove diacríticos (NFC→NFD + strip \pM).
        select
            *,
            lower(
                trim(regexp_replace(normalize(roteiro_raw, nfd), r'\pM', ''))
            ) as roteiro_norm
        from deduplicado
    ),

    final as (
        select
            -- metadados
            id_hash,
            id_hash_original,
            first_seen,
            last_seen,
            updated_at,
            data_particao,

            -- identificadores
            id_qmd,
            id_missao,

            -- pasta KML
            kml_folder,
            kml_folder_raw,

            -- dados
            nome,
            tipo_missao,
            {{ tipo_missao_nome("tipo_missao") }} as tipo_missao_nome,
            tipo_geometria,
            -- Ver macro tipo_operacional para documentação dos sinais e tipos.
            {{
                tipo_operacional(
                    tipo_missao="tipo_missao",
                    tipo_geometria="tipo_geometria",
                    geometry="geometry",
                    roteiro_norm="roteiro_norm",
                    limite_area_m2=limite_area_m2,
                    kml_folder="kml_folder",
                    id_missao="id_missao",
                )
            }} as tipo_operacional,
            -- FALSE para tipo_operacional = 'area' (mesmos sinais duais acima).
            -- TRUE para todo o resto: sede, patrulha, posto e subarea são sempre úteis.
            case
                when
                    lower(trim(tipo_missao)) in ('rf', 'sv', 'sp')
                    and lower(trim(tipo_geometria)) = 'polygon'
                    and (
                        st_area(geometry) > {{ limite_area_m2 }}
                        or regexp_contains(roteiro_norm, r'area da base')
                    )
                then false
                else true
            end as indicador_geometry_util,
            hora_inicio_missao,
            hora_fim_missao,
            roteiro,
            {{ slugify('roteiro') }} as id_roteiro,
            roteiro_raw,
            servicos,
            descricao,
            dados_extendidos,

            -- espacial
            geometria_wkt,
            geometry
        from enriquecido
    ),

    -- Subareas e areas deduplificadas por (id_qmd, geometria_wkt) para join espacial.
    -- Preferência RF sobre SV/SP quando o mesmo polígono é usado por múltiplos tipos.
    -- Garante 1 linha por zona geográfica distinta por QMD.
    subareas as (
        select
            id_qmd,
            geometria_wkt,
            geometry,
            id_roteiro as id_subarea
        from final
        where tipo_operacional = 'subarea'
        qualify
            row_number() over (
                partition by id_qmd, geometria_wkt
                order by case tipo_missao when 'RF' then 1 else 2 end
            ) = 1
    ),

    areas as (
        select
            id_qmd,
            geometria_wkt,
            geometry,
            id_roteiro as id_area
        from final
        where tipo_operacional = 'area'
        qualify
            row_number() over (
                partition by id_qmd, geometria_wkt
                order by case tipo_missao when 'RF' then 1 else 2 end
            ) = 1
    ),

    enriquecido_subarea as (
        select
            f.*,
            coalesce(
                -- subarea identifica-se com sua própria zona
                case when f.tipo_operacional = 'subarea' then f.id_roteiro end,
                -- patrulha/posto/sede: join primário (mesmo QMD + ST_INTERSECTS)
                s_qmd.id_subarea,
                -- patrulha/posto/sede: fallback ST_DISTANCE (qualquer QMD)
                s_geo.id_subarea
            ) as id_subarea
        from final f
        -- Primário: mesmo QMD + ST_INTERSECTS
        left join subareas s_qmd
            on  f.id_qmd = s_qmd.id_qmd
            and f.tipo_operacional in ('patrulha', 'posto', 'sede')
            and st_intersects(f.geometry, s_qmd.geometry)
        -- Fallback: subarea mais próxima de qualquer QMD (sem filtro espacial).
        -- Handles casos onde a feature está na borda ou fora do polígono por precisão.
        -- O QUALIFY ordena por distância e seleciona a mais próxima.
        left join subareas s_geo
            on f.tipo_operacional in ('patrulha', 'posto', 'sede')
        qualify
            row_number() over (
                partition by f.id_hash
                order by
                    -- 1. preferir match do mesmo QMD (ST_INTERSECTS)
                    (s_qmd.id_subarea is not null) desc,
                    -- 2. fallback: subarea mais próxima
                    st_distance(f.geometry, s_geo.geometry) nulls last,
                    -- 3. determinístico
                    coalesce(s_qmd.id_subarea, s_geo.id_subarea) nulls last
            ) = 1
    ),

    enriquecido_area as (
        select
            es.*,
            coalesce(
                -- area identifica-se com sua própria zona
                case when es.tipo_operacional = 'area' then es.id_roteiro end,
                -- patrulha/posto/sede/subarea: join primário (mesmo QMD + ST_INTERSECTS)
                a_qmd.id_area,
                -- patrulha/posto/sede/subarea: fallback ST_DISTANCE (qualquer QMD)
                a_geo.id_area
            ) as id_area
        from enriquecido_subarea es
        -- Primário: mesmo QMD + ST_INTERSECTS
        left join areas a_qmd
            on  es.id_qmd = a_qmd.id_qmd
            and es.tipo_operacional in ('patrulha', 'posto', 'sede', 'subarea')
            and st_intersects(es.geometry, a_qmd.geometry)
        -- Fallback: area mais próxima de qualquer QMD (sem filtro espacial).
        left join areas a_geo
            on es.tipo_operacional in ('patrulha', 'posto', 'sede', 'subarea')
        qualify
            row_number() over (
                partition by es.id_hash
                order by
                    (a_qmd.id_area is not null) desc,
                    st_distance(es.geometry, a_geo.geometry) nulls last,
                    coalesce(a_qmd.id_area, a_geo.id_area) nulls last
            ) = 1
    )

select *
from enriquecido_area