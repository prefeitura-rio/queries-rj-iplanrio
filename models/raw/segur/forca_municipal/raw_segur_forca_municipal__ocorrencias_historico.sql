{{
    config(
        alias="ocorrencias_historico",
        schema="brutos_forca_municipal",
        materialized="table",
        cluster_by=["tipo_ocorrencia_codigo", "id_status"],
    )
}}

with
    source as (
        select *
        from {{ source('brutos_forca_municipal_staging', 'ocorrencias_historico') }}
        where
            safe_cast(data_particao as date) = (
                select max(safe_cast(data_particao as date))
                from {{ source('brutos_forca_municipal_staging', 'ocorrencias_historico') }}
            )
    ),

    renamed as (
        select
            -- metadados da pipeline
            {{ padronize_id('id_hash') }} as id_hash,
            safe_cast(updated_at as datetime) as updated_at,

            -- identificadores (FKs para outras entidades)
            upper({{ padronize_id('AgencyId') }}) as id_agencia,
            {{ padronize_id('Esz') }} as id_esz,
            {{ padronize_id('UserGroupId') }} as id_grupo_usuario,
            {{ padronize_id('CreatedEmployeeId') }} as id_funcionario_criacao,
            {{ padronize_id('OriginalEmployeeId') }} as id_funcionario_original,
            {{ padronize_id('RevisionEmployeeId') }} as id_funcionario_revisao,
            {{ padronize_id('ClosingEmployeeId') }} as id_funcionario_fechamento,

            -- dados
            {{ padronize_id('AgencyEventId') }} as id_ocorrencia,
            upper(safe_cast(AgencyEventTypeCode as string)) as tipo_ocorrencia_codigo,
            {{ proper_br('safe_cast(AgencyEventTypeCodeDesc as string)') }} as tipo_ocorrencia_descricao,
            upper(safe_cast(AgencyEventSubtypeCode as string)) as subtipo_ocorrencia_codigo,
            {{ proper_br('safe_cast(AgencyEventSubtypeCodeDesc as string)') }} as subtipo_ocorrencia_descricao,
            {{ proper_br('safe_cast(EventDescription as string)') }} as descricao_ocorrencia,
            datetime(
                safe_cast(AddedTime as timestamp), 'America/Sao_Paulo'
            ) as data_hora_adicao,
            datetime(
                safe_cast(CreatedTime as timestamp), 'America/Sao_Paulo'
            ) as data_hora_criacao,
            datetime(
                safe_cast(StartedTime as timestamp), 'America/Sao_Paulo'
            ) as data_hora_inicio,
            datetime(
                safe_cast(LastStatusChangeTime as timestamp), 'America/Sao_Paulo'
            ) as data_hora_ultima_mudanca_status,
            datetime(
                safe_cast(PriorityChangedTime as timestamp), 'America/Sao_Paulo'
            ) as data_hora_mudanca_prioridade,
            datetime(
                safe_cast(RevisionTime as timestamp), 'America/Sao_Paulo'
            ) as data_hora_revisao,
            datetime(
                safe_cast(DatabaseInsertTime as timestamp), 'America/Sao_Paulo'
            ) as data_hora_insercao_bd,
            datetime(
                safe_cast(ClosingTime as timestamp), 'America/Sao_Paulo'
            ) as data_hora_fechamento,
            datetime(
                safe_cast(PendingAlarmTime as timestamp), 'America/Sao_Paulo'
            ) as data_hora_alarme_pendente,
            {{ padronize_id('Priority') }} as prioridade,
            {{ padronize_id('AlarmLevel') }} as nivel_alarme,
            {{ padronize_id('StatusCode') }} as id_status,
            {{ padronize_id('SubstatusCode') }} as id_substatus,
            safe_cast(IsOpen as bool) as indicador_aberta,
            safe_cast(IsReopened as bool) as indicador_reaberta,
            safe_cast(safe_cast(TotalAssignedUnits as float64) as int64)
            as total_unidades_atribuidas,
            safe_cast({{ padronize_id('RevisionNumber') }} as int64) as numero_revisao,
            safe_cast(ClosingComment as string) as comentario_fechamento,
            upper(safe_cast(Area as string)) as area_planejamento,
            {{ proper_br('safe_cast(Beat as string)') }} as setor,
            {{ proper_br('safe_cast(District as string)') }} as distrito,
            {{ proper_br('safe_cast(Municipality as string)') }} as municipio,
            {{ proper_br('safe_cast(Zone as string)') }} as zona,
            {{ proper_br('safe_cast(DispatchGroup as string)') }} as grupo_despacho,
            {{ padronize_id('Attributes') }} as atributos,
            safe_cast(CustomData as string) as dados_customizados,
            safe_cast(UserDefinedSupplementalInfo as string) as informacao_suplementar,
            upper(safe_cast(AgencyEventTypeCode as string)) = 'DM' as indicador_desvio_missao,

            -- partição
            safe_cast(data_particao as date) as data_particao

        from source
    )

select *
from renamed
