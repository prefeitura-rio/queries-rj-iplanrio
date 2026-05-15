{{
    config(
        alias="ocorrencias_ativas_v2",
        schema="brutos_forca_municipal",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
        cluster_by=["tipo_ocorrencia_codigo", "id_status"],
    )
}}

with
    source as (
        select *
        from {{ source('brutos_forca_municipal_staging', 'ocorrencias_ativas_v2') }}
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
            upper({{ padronize_id('PrimaryUnitId') }}) as id_unidade_primaria,
            {{ padronize_id('Esz') }} as id_esz,
            {{ padronize_id('UserGroupId') }} as id_grupo_usuario,
            {{ padronize_id('CreatedEmployeeId') }} as id_funcionario_criacao,
            {{ padronize_id('PrimaryEmployeeId') }} as id_funcionario_primario,
            {{ padronize_id('ClosingEmployeeId') }} as id_funcionario_fechamento,

            -- dados
            {{ padronize_id('AgencyEventId') }} as id_ocorrencia,
            upper(safe_cast(AgencyEventTypeCode as string)) as tipo_ocorrencia_codigo,
            {{ proper_br('safe_cast(AgencyEventTypeCodeDesc as string)') }} as tipo_ocorrencia_descricao,
            upper(safe_cast(AgencyEventSubtypeCode as string)) as subtipo_ocorrencia_codigo,
            {{ proper_br('safe_cast(AgencyEventSubtypeCodeDesc as string)') }} as subtipo_ocorrencia_descricao,
            {{ proper_br('safe_cast(EventDescription as string)') }} as descricao_ocorrencia,
            datetime(
                safe_cast(CreatedTime as timestamp), 'America/Sao_Paulo'
            ) as data_hora_criacao,
            datetime(
                safe_cast(FirstUnitDispatchedTime as timestamp), 'America/Sao_Paulo'
            ) as data_hora_primeiro_despacho,
            datetime(
                safe_cast(FirstUnitEnroutedTime as timestamp), 'America/Sao_Paulo'
            ) as data_hora_primeira_unidade_a_caminho,
            datetime(
                safe_cast(FirstUnitArrivedTime as timestamp), 'America/Sao_Paulo'
            ) as data_hora_primeira_chegada,
            datetime(
                safe_cast(LastStatusChangeTime as timestamp), 'America/Sao_Paulo'
            ) as data_hora_ultima_mudanca_status,
            datetime(
                safe_cast(PriorityChangedTime as timestamp), 'America/Sao_Paulo'
            ) as data_hora_mudanca_prioridade,
            datetime(
                safe_cast(DatabaseUpdateTime as timestamp), 'America/Sao_Paulo'
            ) as data_hora_atualizacao_bd,
            datetime(
                safe_cast(ClosingTime as timestamp), 'America/Sao_Paulo'
            ) as data_hora_fechamento,
            {{ padronize_id('Priority') }} as prioridade,
            {{ padronize_id('StatusCode') }} as id_status,
            safe_cast(ClosingComment as string) as comentario_fechamento,
            upper(safe_cast(Area as string)) as area,
            {{ proper_br('safe_cast(Beat as string)') }} as setor,
            {{ proper_br('safe_cast(District as string)') }} as distrito,
            {{ proper_br('safe_cast(Zone as string)') }} as zona,
            {{ proper_br('safe_cast(DispatchGroup as string)') }} as grupo_despacho,
            {{ proper_br('safe_cast(Location as string)') }} as localizacao,
            safe_cast(Agents as string) as agentes,
            {{ padronize_id('Attributes') }} as atributos,
            safe_cast(CustomData as string) as dados_customizados,

            -- espacial
            safe_cast(Latitude as float64) as latitude,
            safe_cast(Longitude as float64) as longitude,
            st_geogpoint(
                safe_cast(Longitude as float64),
                safe_cast(Latitude as float64)
            ) as geometry,

            -- partição
            safe_cast(data_particao as date) as data_particao

        from source
    )

select *
from renamed
