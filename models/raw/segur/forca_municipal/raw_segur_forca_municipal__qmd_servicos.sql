{{
    config(
        alias="qmd_servicos",
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
        from {{ source('brutos_forca_municipal_staging', 'qmd_servicos') }}
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
            {{ padronize_id('IdPlano') }} as id_plano,
            {{ padronize_id('IdQmd') }} as id_qmd,

            -- dados
            {{ padronize_id('Id') }} as id_servico,
            upper(trim(safe_cast(Nome as string))) as id_unidade,
            safe_cast(Dias as string) as dias,
            regexp_extract(upper(trim(safe_cast(Nome as string))), r'^([A-Z]+)\d') as tipo_unidade,
            regexp_extract(upper(trim(safe_cast(Nome as string))), r'-(.+)$')      as base_operacional,

            -- partição
            safe_cast(data_particao as date) as data_particao

        from source
    )

select *
from renamed
