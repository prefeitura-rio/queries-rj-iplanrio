{{
    config(
        alias="qmd_geometria_kml",
        schema="brutos_forca_municipal",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id_hash",
        merge_update_columns=["last_seen", "data_particao", "updated_at"],
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
            {{ padronize_id("id_hash") }} as id_hash,
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
            safe_cast(geometry_type as string) as tipo_geometria,
            safe.parse_time(
                '%H:%M', json_value(safe_cast(extended_data as string), '$.HoraInicio')
            ) as hora_inicio_missao,
            safe.parse_time(
                '%H:%M', json_value(safe_cast(extended_data as string), '$.HoraFim')
            ) as hora_fim_missao,
            -- roteiro_raw: valor bruto de $.Roteiro, apenas trim. Usado como sinal
            -- de redundância textual na classificação de tipo_operacional.
            trim(json_value(safe_cast(extended_data as string), '$.Roteiro')) as roteiro_raw,
            -- roteiro: remove prefixo de tipo (ex: "RF\t", "PTR_01\t") e prefixo
            -- "Dentro da Subárea" (inconsistência na fonte — mesmo geometry, nomes
            -- distintos)
            regexp_replace(
                regexp_replace(
                    trim(json_value(safe_cast(extended_data as string), '$.Roteiro')),
                    r'^(?:RF|PTR|PB|SV|SP)(?:\s*_\d+)?\t',
                    ''
                ),
                r'(?i)^Dentro da Sub[aá]rea\s*',
                ''
            ) as roteiro,
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
            any_value(geometry) as geometry
        from renamed
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
            tipo_geometria,
            -- Classificação operacional da feature — múltiplos sinais para robustez:
            --   kml_folder     : distingue sede (qmd) de missão (missoes)
            --   id_missao      : NULL exclusivamente para sedes QMD
            --   tipo_missao    : código operacional (PTR, PB, RF, SV, SP)
            --   tipo_geometria : consistente com tipo_missao (validação cruzada)
            -- Para RF/SV/SP Polygon, dois sinais redundantes definem area vs subarea:
            --   st_area        : sinal primário — gap confirmado em staging (1.7→295 km²)
            --   roteiro_norm   : sinal secundário — padrões exclusivos por cluster:
            --                    'area da base'     → exclusivo de polígonos grandes
            --                    'dentro da subarea'→ exclusivo de polígonos pequenos
            -- Um sinal basta (OR). Se ambos discordarem, CASE ordering decide: area primeiro.
            -- NULL indica combinação de sinais não mapeada — dado fonte inconsistente.
            case
                -- Sede QMD: pasta qmd E missão nula (dois sinais)
                when lower(trim(kml_folder)) = 'qmd' and id_missao is null
                then 'sede'
                -- Patrulha PTR: tipo_missao E geometria LineString
                when
                    lower(trim(tipo_missao)) = 'ptr'
                    and lower(trim(tipo_geometria)) = 'linestring'
                then 'patrulha'
                -- Posto fixo PB: tipo_missao E geometria Point
                when
                    lower(trim(tipo_missao)) = 'pb'
                    and lower(trim(tipo_geometria)) = 'point'
                then 'posto'
                -- Área de base inteira: sinal primário (> 10 km²) OU texto 'area da base'
                when
                    lower(trim(tipo_missao)) in ('rf', 'sv', 'sp')
                    and lower(trim(tipo_geometria)) = 'polygon'
                    and (
                        st_area(geometry) > {{ limite_area_m2 }}
                        or regexp_contains(roteiro_norm, r'area da base')
                    )
                then 'area'
                -- Subárea operacional: sinal primário (≤ 10 km²) OU texto 'dentro da subarea'
                when
                    lower(trim(tipo_missao)) in ('rf', 'sv', 'sp')
                    and lower(trim(tipo_geometria)) = 'polygon'
                    and (
                        st_area(geometry) <= {{ limite_area_m2 }}
                        or regexp_contains(roteiro_norm, r'dentro da subarea')
                    )
                then 'subarea'
            end as tipo_operacional,
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
            roteiro_raw,
            servicos,
            descricao,
            dados_extendidos,

            -- espacial
            geometria_wkt,
            geometry
        from enriquecido
    )

select *
from final