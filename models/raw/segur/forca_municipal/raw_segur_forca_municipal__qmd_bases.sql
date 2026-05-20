{{
    config(
        alias="qmd_bases",
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
        from {{ source("brutos_forca_municipal_staging", "qmd_kml") }}
        where
            lower(regexp_replace(normalize(kml_folder, nfd), r'\pM', ''))
            in ('qmds', 'qmd')
            {% if is_incremental() %}
                and safe_cast(data_particao as date) = (
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

            -- dados
            {{ proper_br("safe_cast(name as string)") }} as nome,
            safe_cast(geometry_type as string) as tipo_geometria,
            safe_cast(description as string) as descricao,

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
            max(data_particao) as data_particao,
            max(updated_at) as updated_at,

            -- identificadores
            any_value(id_qmd) as id_qmd,

            -- dados
            any_value(nome) as nome,
            any_value(tipo_geometria) as tipo_geometria,
            any_value(descricao) as descricao,

            -- espacial
            any_value(geometria_wkt) as geometria_wkt,
            any_value(geometry) as geometry
        from renamed
        group by id_hash
    )

select *
from deduplicado