{{
    config(
        alias="qmd_kml",
        schema="brutos_forca_municipal",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
        cluster_by=["id_qmd"],
    )
}}

with
    source as (
        select *
        from {{ source('brutos_forca_municipal_staging', 'qmd_kml') }}
        {% if is_incremental() %}
            where
                safe_cast(data_particao as date)
                >= (select max(data_particao) from {{ this }})
        {% endif %}
    ),

    renamed as (
        select
            -- metadados da pipeline
            {{ padronize_id('id_hash') }} as id_hash,
            safe_cast(updated_at as datetime) as updated_at,

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
            st_geogfromtext(safe_cast(geometry_wkt as string)) as geometry,

            -- partição
            safe_cast(data_particao as date) as data_particao

        from source
    )

select *
from renamed
