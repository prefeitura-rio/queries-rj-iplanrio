{{
    config(
        alias="qmd_plano",
        schema="brutos_forca_municipal",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
        cluster_by=["id_responsavel_criacao"],
    )
}}

with
    source as (
        select *
        from {{ source('brutos_forca_municipal_staging', 'qmd_plano') }}
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
            {{ padronize_id('IdRespCriacao') }} as id_responsavel_criacao,

            -- dados
            {{ padronize_id('Id') }} as id_plano,
            {{ proper_br('safe_cast(Nome as string)') }} as nome,
            datetime(
                safe_cast(SemanaReferenciaInicio as timestamp), 'America/Sao_Paulo'
            ) as data_hora_semana_referencia_inicio,
            datetime(
                safe_cast(SemanaReferenciaFim as timestamp), 'America/Sao_Paulo'
            ) as data_hora_semana_referencia_fim,
            datetime(
                safe_cast(DataHoraCriacao as timestamp), 'America/Sao_Paulo'
            ) as data_hora_criacao,

            -- partição
            safe_cast(data_particao as date) as data_particao

        from source
    )

select *
from renamed
