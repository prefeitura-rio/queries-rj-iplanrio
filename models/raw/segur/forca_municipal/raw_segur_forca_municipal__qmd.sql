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
        cluster_by=["indicador_ativo"],
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
            {{ proper_br('safe_cast(Area as string)') }} as localizacao_patrulha,
            datetime(
                safe_cast(DataVigenciaInicio as timestamp), 'America/Sao_Paulo'
            ) as data_hora_vigencia_inicio,
            datetime(
                safe_cast(DataVigenciaFim as timestamp), 'America/Sao_Paulo'
            ) as data_hora_vigencia_fim,
            parse_time('%H:%M', HoraExecucaoInicio) as hora_inicio_qmd,
            parse_time('%H:%M', HoraExecucaoFim)    as hora_fim_qmd,
            datetime(
                safe_cast(regexp_replace(DataHoraCriacao, r'(\.\d{6})\d+', r'\1') as timestamp),
                'America/Sao_Paulo'
            ) as data_hora_criacao,
            datetime(
                safe_cast(regexp_replace(DataHoraAutorizacao, r'(\.\d{6})\d+', r'\1') as timestamp),
                'America/Sao_Paulo'
            ) as data_hora_autorizacao,
            safe_cast(StatusAtivo as bool) as indicador_ativo,
            safe_cast(StatusValido as bool) as indicador_valido,
            safe_cast(StatusAutorizado as bool) as indicador_autorizado,
            safe_cast(Resumo as string) as resumo,
            safe_cast(Prescricoes as string) as prescricoes,
            parse_time('%H:%M', HoraExecucaoFim) < parse_time('%H:%M', HoraExecucaoInicio)
                as indicador_hora_cruza_meia_noite,

            -- partição
            safe_cast(data_particao as date) as data_particao

        from source
    ),

    com_duracao as (
        select
            *,
            if(
                indicador_hora_cruza_meia_noite,
                time_diff(hora_fim_qmd, hora_inicio_qmd, minute) + 24 * 60,
                time_diff(hora_fim_qmd, hora_inicio_qmd, minute)
            ) as duracao_minutos_qmd
        from renamed
    )

select *
from com_duracao
