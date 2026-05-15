{{
    config(
        alias="unidades_ativas",
        schema="brutos_forca_municipal",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

with
    source as (
        select *
        from {{ source("brutos_forca_municipal_staging", "unidades_ativas") }}
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
            upper({{ padronize_id('AgencyId') }}) as id_agencia,
            upper({{ padronize_id('UnitId') }}) as id_unidade,

            -- dados
            datetime(
                safe_cast(LogonTime as timestamp), 'America/Sao_Paulo'
            ) as data_hora_logon,
            datetime(
                safe_cast(StatusChangeTime as timestamp), 'America/Sao_Paulo'
            ) as data_hora_mudanca_status,

            {{ padronize_id('AssignedAgencyEventId') }} as id_ocorrencia_atribuida,
            {{ padronize_id('StatusedAgencyEventId') }} as id_ocorrencia_status,
            upper(safe_cast(StatusedAgencyEventTypeCode as string)) as tipo_ocorrencia_status_codigo,
            upper(safe_cast(StatusedAgencyEventSubtypeCode as string)) as subtipo_ocorrencia_status_codigo,

            {{ proper_br('safe_cast(Beat as string)') }} as setor,
            {{ proper_br('safe_cast(DispatchGroup as string)') }} as grupo_despacho,
            {{ proper_br('safe_cast(Zone as string)') }} as zona,
            upper(safe_cast(UnitType as string)) as tipo_unidade,
            upper(safe_cast(OutOfServiceTypeCode as string)) as codigo_saida_servico,
            {{ padronize_id('Status') }} as status,
            {{ padronize_id('DefaultAvailableStatus') }} as status_disponibilidade_padrao,
            {{ padronize_id('DispatchAlarmLevel') }} as nivel_alarme_despacho,
            safe_cast(AlarmTime as int64) as tempo_alarme,
            safe_cast(DelayTime as int64) as tempo_atraso,
            safe_cast(safe_cast(TotalEventTime as float64) as int64) as tempo_total_ocorrencias,
            safe_cast(safe_cast(TotalUnavailableTime as float64) as int64) as tempo_total_indisponivel,
            safe_cast(UpdateCount as int64) as contador_atualizacoes,
            safe_cast(IsUnavailable as bool) as indicador_indisponivel,
            safe_cast(ChangeComment as string) as comentario_alteracao,
            {{ proper_br('safe_cast(Location as string)') }} as localizacao,
            safe_cast(CustomData as string) as dados_customizados,

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
    )

select *
from renamed
