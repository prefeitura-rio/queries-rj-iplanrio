{{
    config(
        alias="qmd",
        schema="brutos_forca_municipal",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
        cluster_by=["indicador_ativo", "area"],
    )
}}

with
    source as (
        select *
        from {{ source('brutos_forca_municipal_staging', 'qmd') }}
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
            {{ padronize_id('IdRespAutorizacao') }} as id_responsavel_autorizacao,

            -- dados
            {{ padronize_id('Id') }} as id_qmd,
            {{ proper_br('safe_cast(Nome as string)') }} as nome,
            {{ proper_br('safe_cast(Area as string)') }} as area,
            datetime(
                safe_cast(DataVigenciaInicio as timestamp), 'America/Sao_Paulo'
            ) as data_hora_vigencia_inicio,
            datetime(
                safe_cast(DataVigenciaFim as timestamp), 'America/Sao_Paulo'
            ) as data_hora_vigencia_fim,
            safe_cast(HoraExecucaoInicio as time) as hora_execucao_inicio,
            safe_cast(HoraExecucaoFim as time) as hora_execucao_fim,
            datetime(
                safe_cast(DataHoraCriacao as timestamp), 'America/Sao_Paulo'
            ) as data_hora_criacao,
            datetime(
                safe_cast(DataHoraAutorizacao as timestamp), 'America/Sao_Paulo'
            ) as data_hora_autorizacao,
            safe_cast(StatusAtivo as bool) as indicador_ativo,
            safe_cast(StatusValido as bool) as indicador_valido,
            safe_cast(StatusAutorizado as bool) as indicador_autorizado,
            safe_cast(Resumo as string) as resumo,
            safe_cast(Prescricoes as string) as prescricoes,

            -- partição
            safe_cast(data_particao as date) as data_particao

        from source
    )

select *
from renamed
