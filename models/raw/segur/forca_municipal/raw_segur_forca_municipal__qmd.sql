{{
    config(
        alias="qmd",
        schema="brutos_forca_municipal",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id_hash",
        merge_update_columns=["last_seen", "data_particao", "updated_at"],
        cluster_by=["indicador_ativo"],
    )
}}

with
    source as (
        select *
        from {{ source('brutos_forca_municipal_staging', 'qmd') }}
        {% if is_incremental() %}
            -- runs incrementais: só a última partição disponível
            where safe_cast(data_particao as date) = (
                select max(safe_cast(data_particao as date))
                from {{ source('brutos_forca_municipal_staging', 'qmd') }}
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
                as indicador_hora_cruza_meia_noite

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
            any_value(id_responsavel_criacao)      as id_responsavel_criacao,
            any_value(id_responsavel_autorizacao)  as id_responsavel_autorizacao,

            -- dados
            any_value(id_qmd)                        as id_qmd,
            any_value(nome)                          as nome,
            any_value(localizacao_patrulha)          as localizacao_patrulha,
            any_value(data_hora_vigencia_inicio)     as data_hora_vigencia_inicio,
            any_value(data_hora_vigencia_fim)        as data_hora_vigencia_fim,
            any_value(hora_inicio_qmd)               as hora_inicio_qmd,
            any_value(hora_fim_qmd)                  as hora_fim_qmd,
            any_value(data_hora_criacao)             as data_hora_criacao,
            any_value(data_hora_autorizacao)         as data_hora_autorizacao,
            any_value(indicador_ativo)               as indicador_ativo,
            any_value(indicador_valido)              as indicador_valido,
            any_value(indicador_autorizado)          as indicador_autorizado,
            any_value(resumo)                        as resumo,
            any_value(prescricoes)                   as prescricoes,
            any_value(indicador_hora_cruza_meia_noite) as indicador_hora_cruza_meia_noite,
            any_value(duracao_minutos_qmd)           as duracao_minutos_qmd
        from com_duracao
        group by id_hash
    )

select *
from deduplicado
