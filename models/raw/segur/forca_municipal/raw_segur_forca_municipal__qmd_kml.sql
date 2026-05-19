{{
    config(
        alias="qmd_kml",
        schema="brutos_forca_municipal",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id_hash",
        merge_update_columns=["last_seen", "data_particao", "updated_at"],
        cluster_by=["id_qmd"],
    )
}}

with
    source as (
        select *
        from {{ source('brutos_forca_municipal_staging', 'qmd_kml') }}
        {% if is_incremental() %}
            -- runs incrementais: só a última partição disponível
            where safe_cast(data_particao as date) = (
                select max(safe_cast(data_particao as date))
                from {{ source('brutos_forca_municipal_staging', 'qmd_kml') }}
            )
        {% endif %}
    ),

    renamed as (
        select
            -- metadados da pipeline
            {{ padronize_id('id_hash') }} as id_hash,
            safe_cast(updated_at as datetime) as updated_at,
            safe_cast(data_particao as date) as data_particao,

            -- identificadores (FKs para outras entidades)
            {{ padronize_id('qmd_id') }} as id_qmd,

            -- dados
            case kml_folder
                when 'QMDs'    then 'QMD'
                when 'Missões' then 'MISSAO'
                else upper(safe_cast(kml_folder as string))
            end as pasta_kml,
            {{ proper_br('safe_cast(name as string)') }} as nome,
            safe_cast(geometry_type as string) as tipo_geometria,
            safe_cast(description as string) as descricao,
            safe_cast(extended_data as string) as dados_extendidos,

            -- dados derivados de dados_extendidos (válidos para pasta_kml = 'MISSAO')
            json_value(safe_cast(extended_data as string), '$.MissaoId') as id_missao,
            json_value(safe_cast(extended_data as string), '$.Tipo')     as tipo_missao,
            safe.parse_time(
                '%H:%M', json_value(safe_cast(extended_data as string), '$.HoraInicio')
            ) as hora_inicio_missao,
            safe.parse_time(
                '%H:%M', json_value(safe_cast(extended_data as string), '$.HoraFim')
            ) as hora_fim_missao,

            -- espacial
            safe_cast(geometry_wkt as string) as geometria_wkt,
            st_geogfromtext(safe_cast(geometry_wkt as string)) as geometry

        from source
    ),

    deduplicado as (
        select
            -- metadados da pipeline
            id_hash,
            min(updated_at) as first_seen,
            max(updated_at) as last_seen,
            max(data_particao) as data_particao,
            max(updated_at)    as updated_at,

            -- identificadores
            any_value(id_qmd) as id_qmd,

            -- dados
            any_value(pasta_kml)       as pasta_kml,
            any_value(nome)            as nome,
            any_value(tipo_geometria)  as tipo_geometria,
            any_value(descricao)       as descricao,
            any_value(dados_extendidos) as dados_extendidos,
            any_value(id_missao)       as id_missao,
            any_value(tipo_missao)     as tipo_missao,
            any_value(hora_inicio_missao) as hora_inicio_missao,
            any_value(hora_fim_missao)    as hora_fim_missao,

            -- espacial
            any_value(geometria_wkt) as geometria_wkt,
            any_value(geometry)      as geometry
        from renamed
        group by id_hash
    )

select *
from deduplicado
