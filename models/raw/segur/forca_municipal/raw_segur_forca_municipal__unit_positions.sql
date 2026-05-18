{{
    config(
        alias="unit_positions",
        schema="brutos_forca_municipal",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
        cluster_by=["id_unidade"],
    )
}}

with
    source as (
        select *
        from {{ source("brutos_forca_municipal_staging", "unit_positions") }}
        {% if is_incremental() %}
            where safe_cast(data_particao as date) >= (
                select max(data_particao) from {{ this }}
            )
        {% endif %}
    ),

    renamed as (
        select
            -- metadados da pipeline
            {{ padronize_id('id_hash') }} as id_hash,
            safe_cast(updated_at as datetime) as updated_at,

            -- identificadores
            upper({{ padronize_id('UnitId') }}) as id_unidade,

            -- dados
            datetime(safe_cast(Date as timestamp), 'America/Sao_Paulo') as data_hora,
            safe_cast(data_coleta as date) as data_coleta,
            regexp_extract(upper(trim(safe_cast(UnitId as string))), r'^([A-Z]+)\d') as tipo_unidade,
            regexp_extract(upper(trim(safe_cast(UnitId as string))), r'-(.+)$')      as base_operacional,

            -- espacial
            safe_cast(Latitude as float64) as latitude,
            safe_cast(Longitude as float64) as longitude,
            st_geogpoint(
                safe_cast(Longitude as float64),
                safe_cast(Latitude as float64)
            ) as geometry,

            -- partição
            safe_cast(data_particao as date) as data_particao,
        from source
    ),

    com_hora as (
        select
            *,
            time(data_hora) as hora_leitura
        from renamed
    )

select *
from com_hora
