{{
    config(
        alias="unidades_historico",
        schema="brutos_forca_municipal",
        materialized="table",
        cluster_by=["id_unidade"],
    )
}}

with
    source as (
        select *
        from {{ source("brutos_forca_municipal_staging", "unidades_historico") }}
        where
            safe_cast(data_particao as date) = (
                select max(safe_cast(data_particao as date))
                from
                    {{ source("brutos_forca_municipal_staging", "unidades_historico") }}
            )
    ),

    renamed as (
        select
            -- metadados da pipeline
            {{ padronize_id("id_hash") }} as id_hash,
            safe_cast(updated_at as datetime) as updated_at,

            -- identificadores (FKs para outras entidades)
            upper({{ padronize_id("AgencyId") }}) as id_agencia,
            upper({{ padronize_id("UnitId") }}) as id_unidade,
            {{ padronize_id("StationId") }} as id_estacao,
            {{ padronize_id("AssignedAgencyEventId") }} as id_ocorrencia_atribuida,
            {{ padronize_id("StatusedAgencyEventId") }} as id_ocorrencia_status,
            {{ padronize_id("CreatedEmployeeId") }} as id_funcionario_criacao,

            -- dados
            {{ padronize_id("ActiveUnitHistoryId") }} as id_historico_unidade,
            {{ padronize_id("ActionCode") }} as id_acao,
            upper(safe_cast(unittype as string)) as tipo_unidade,
            upper(
                safe_cast(statusedagencyeventtypecode as string)
            ) as tipo_ocorrencia_status_codigo,
            upper(
                safe_cast(statusedagencyeventsubtypecode as string)
            ) as subtipo_ocorrencia_status_codigo,
            safe_cast(StatusedAgencyEventRevisionNum as int64) as numero_revisao_ocorrencia_status,
            datetime(
                safe_cast(createdtime as timestamp), 'America/Sao_Paulo'
            ) as data_hora_criacao,
            datetime(
                safe_cast(logontime as timestamp), 'America/Sao_Paulo'
            ) as data_hora_logon,
            datetime(
                safe_cast(databaseinserttime as timestamp), 'America/Sao_Paulo'
            ) as data_hora_insercao_bd,
            upper({{ padronize_id("Status") }}) as id_status,
            {{ padronize_id("DefaultAvailableStatus") }}
            as id_status_disponibilidade_padrao,
            {{ padronize_id("DispatchAlarmLevel") }} as nivel_alarme_despacho,
            safe_cast(isunavailable as bool) as indicador_indisponivel,
            safe_cast(areemployeestracked as bool) as indicador_funcionarios_rastreados,
            upper(safe_cast(outofservicetypecode as string)) as tipo_saida_servico,
            {{ proper_br("safe_cast(Beat as string)") }} as setor,
            {{ proper_br("safe_cast(DispatchGroup as string)") }} as grupo_despacho,
            {{ proper_br("safe_cast(Zone as string)") }} as zona,
            {{ proper_br("safe_cast(LineupName as string)") }} as nome_escalacao,
            {{ proper_br("safe_cast(Location as string)") }} as localizacao,
            safe_cast(orderwithincreatedtime as int64) as ordem_criacao,
            safe_cast(alarmtime as int64) as tempo_alarme,
            safe_cast(delaytime as int64) as tempo_atraso,
            safe_cast(
                safe_cast(totaleventtime as float64) as int64
            ) as tempo_total_ocorrencias,
            safe_cast(
                safe_cast(totalunavailabletime as float64) as int64
            ) as tempo_total_indisponivel,
            safe_cast(
                safe_cast(totalavailablestationtime as float64) as int64
            ) as tempo_total_disponivel_estacao,
            safe_cast(totaleventcount as int64) as total_ocorrencias,
            safe_cast(changecomment as string) as comentario_alteracao,
            safe_cast(customdata as string) as dados_customizados,
            {{ base_operacional('UnitId') }} as base_operacional,

            -- espacial
            safe_cast(latitude as float64) as latitude,
            safe_cast(longitude as float64) as longitude,
            st_geogpoint(
                safe_cast(longitude as float64), safe_cast(latitude as float64)
            ) as geometry,

            -- partição
            safe_cast(data_particao as date) as data_particao

        from source
    ),

    com_derivados as (
        select
            *,
            id_status in ('DP', 'QE')  as indicador_despachada,
            date(data_hora_logon)       as data_logon
        from renamed
    )

select *
from com_derivados