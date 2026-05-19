{{
    config(
        alias="qmd_detalhes",
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
        from {{ source('brutos_forca_municipal_staging', 'qmd_detalhes') }}
        {% if is_incremental() %}
            -- runs incrementais: só a última partição disponível
            where safe_cast(data_particao as date) = (
                select max(safe_cast(data_particao as date))
                from {{ source('brutos_forca_municipal_staging', 'qmd_detalhes') }}
            )
        {% endif %}
    ),

    renamed as (
        select
            -- metadados da pipeline
            {{ padronize_id('id_hash') }} as id_hash,
            safe_cast(updated_at as datetime) as updated_at,
            safe_cast(data_particao as date) as data_particao,

            -- dados
            {{ padronize_id('qmdId') }} as id_qmd,
            {{ proper_br('safe_cast(qmdNome as string)') }} as nome,
            {{ proper_br('safe_cast(qmdArea as string)') }} as localizacao_patrulha,
            datetime(
                safe_cast(qmdDataVigenciaInicio as timestamp), 'America/Sao_Paulo'
            ) as data_hora_vigencia_inicio,
            datetime(
                safe_cast(qmdDataVigenciaFim as timestamp), 'America/Sao_Paulo'
            ) as data_hora_vigencia_fim,
            lower(trim(qmdStatusAtivo)) = 'sim' as indicador_ativo,
            lower(trim(qmdStatusAutorizado)) = 'sim' as indicador_autorizado,
            lower(trim(qmdStatusValido)) = 'sim' as indicador_valido,
            safe_cast(qmdResumo as string) as resumo,
            safe_cast(missoes as string) as missoes

        from source
    ),

    deduplicado as (
        select
            -- metadados da pipeline
            id_hash,
            min(updated_at) as first_seen,
            max(updated_at) as last_seen,
            max(data_particao) as data_particao,
            max(updated_at)    as updated_at,

            -- dados
            any_value(id_qmd)                    as id_qmd,
            any_value(nome)                      as nome,
            any_value(localizacao_patrulha)      as localizacao_patrulha,
            any_value(data_hora_vigencia_inicio) as data_hora_vigencia_inicio,
            any_value(data_hora_vigencia_fim)    as data_hora_vigencia_fim,
            any_value(indicador_ativo)           as indicador_ativo,
            any_value(indicador_autorizado)      as indicador_autorizado,
            any_value(indicador_valido)          as indicador_valido,
            any_value(resumo)                    as resumo,
            any_value(missoes)                   as missoes
        from renamed
        group by id_hash
    )

select *
from deduplicado
