{{
    config(
        alias="ocorrencias_ativas_v2",
        schema="brutos_forca_municipal",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id_hash",
        merge_update_columns=["last_seen", "data_particao", "updated_at"],
        cluster_by=["tipo_ocorrencia_codigo", "id_status"],
    )
}}

with
    source as (
        select *
        from {{ source('brutos_forca_municipal_staging', 'ocorrencias_ativas_v2') }}
        {% if is_incremental() %}
            -- runs incrementais: só a última partição disponível na staging
            where safe_cast(data_particao as date) = (
                select max(safe_cast(data_particao as date))
                from {{ source('brutos_forca_municipal_staging', 'ocorrencias_ativas_v2') }}
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
            upper(safe_cast(Area as string)) as area_planejamento,
            {{ proper_br('safe_cast(Beat as string)') }} as setor,
            {{ proper_br('safe_cast(District as string)') }} as distrito,
            {{ proper_br('safe_cast(Zone as string)') }} as zona,
            {{ proper_br('safe_cast(DispatchGroup as string)') }} as grupo_despacho,
            {{ proper_br('safe_cast(Location as string)') }} as localizacao,
            safe_cast(Agents as string) as agentes,
            {{ padronize_id('Attributes') }} as atributos,
            safe_cast(CustomData as string) as dados_customizados,
            upper(safe_cast(AgencyEventTypeCode as string)) = 'DM'                as indicador_desvio_missao,
            upper(safe_cast(AgencyEventTypeCode as string)) in ('DM', 'AGD')    as indicador_evento_operacional,

            -- espacial
            safe_cast(Latitude as float64) as latitude,
            safe_cast(Longitude as float64) as longitude,
            st_geogpoint(
                safe_cast(Longitude as float64),
                safe_cast(Latitude as float64)
            ) as geometry

        from source
    ),

    derivados as (
        select
            *,
            datetime_diff(
                data_hora_primeiro_despacho, data_hora_criacao, second
            )                                                                      as ttd_segundos,
            datetime_diff(
                data_hora_primeira_unidade_a_caminho, data_hora_primeiro_despacho, second
            )                                                                      as tter_segundos,
            datetime_diff(
                data_hora_primeira_chegada, data_hora_primeiro_despacho, second
            )                                                                      as ttoa_segundos
        from renamed
    ),

    deduplicado as (
        select
            -- metadados da pipeline
            id_hash,
            min(updated_at)     as first_seen,
            max(updated_at)     as last_seen,
            max(data_particao)  as data_particao,
            max(updated_at)     as updated_at,

            -- identificadores
            any_value(id_unidade_primaria)      as id_unidade_primaria,
            any_value(id_esz)                   as id_esz,
            any_value(id_grupo_usuario)         as id_grupo_usuario,
            any_value(id_funcionario_criacao)   as id_funcionario_criacao,
            any_value(id_funcionario_primario)  as id_funcionario_primario,
            any_value(id_funcionario_fechamento) as id_funcionario_fechamento,

            -- dados
            any_value(id_ocorrencia)                        as id_ocorrencia,
            any_value(tipo_ocorrencia_codigo)               as tipo_ocorrencia_codigo,
            any_value(tipo_ocorrencia_descricao)            as tipo_ocorrencia_descricao,
            any_value(subtipo_ocorrencia_codigo)            as subtipo_ocorrencia_codigo,
            any_value(subtipo_ocorrencia_descricao)         as subtipo_ocorrencia_descricao,
            any_value(descricao_ocorrencia)                 as descricao_ocorrencia,
            any_value(data_hora_criacao)                    as data_hora_criacao,
            any_value(data_hora_primeiro_despacho)          as data_hora_primeiro_despacho,
            any_value(data_hora_primeira_unidade_a_caminho) as data_hora_primeira_unidade_a_caminho,
            any_value(data_hora_primeira_chegada)           as data_hora_primeira_chegada,
            any_value(data_hora_ultima_mudanca_status)      as data_hora_ultima_mudanca_status,
            any_value(data_hora_mudanca_prioridade)         as data_hora_mudanca_prioridade,
            any_value(data_hora_atualizacao_bd)             as data_hora_atualizacao_bd,
            any_value(data_hora_fechamento)                 as data_hora_fechamento,
            any_value(prioridade)                           as prioridade,
            any_value(id_status)                            as id_status,
            any_value(comentario_fechamento)                as comentario_fechamento,
            any_value(area_planejamento)                    as area_planejamento,
            any_value(setor)                                as setor,
            any_value(distrito)                             as distrito,
            any_value(zona)                                 as zona,
            any_value(grupo_despacho)                       as grupo_despacho,
            any_value(localizacao)                          as localizacao,
            any_value(agentes)                              as agentes,
            any_value(atributos)                            as atributos,
            any_value(dados_customizados)                   as dados_customizados,
            any_value(indicador_desvio_missao)              as indicador_desvio_missao,
            any_value(indicador_evento_operacional)         as indicador_evento_operacional,

            -- espacial
            any_value(latitude)     as latitude,
            any_value(longitude)    as longitude,
            any_value(geometry)     as geometry,

            -- derivados
            any_value(ttd_segundos)     as ttd_segundos,
            any_value(tter_segundos)    as tter_segundos,
            any_value(ttoa_segundos)    as ttoa_segundos

        from derivados
        group by id_hash
    )

select *
from deduplicado
