{{
    config(
        alias="qmd_detalhes",
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
        from {{ source('brutos_forca_municipal_staging', 'qmd_detalhes') }}
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

            -- dados
            {{ padronize_id('qmdId') }} as id_qmd,
            {{ proper_br('safe_cast(qmdNome as string)') }} as nome,
            {{ proper_br('safe_cast(qmdArea as string)') }} as area,
            datetime(
                safe_cast(qmdDataVigenciaInicio as timestamp), 'America/Sao_Paulo'
            ) as data_hora_vigencia_inicio,
            datetime(
                safe_cast(qmdDataVigenciaFim as timestamp), 'America/Sao_Paulo'
            ) as data_hora_vigencia_fim,
            qmdStatusAtivo = 'Sim' as indicador_ativo,
            qmdStatusAutorizado = 'Sim' as indicador_autorizado,
            qmdStatusValido = 'Sim' as indicador_valido,
            safe_cast(qmdResumo as string) as resumo,
            safe_cast(missoes as string) as missoes,

            -- partição
            safe_cast(data_particao as date) as data_particao

        from source
    )

select *
from renamed
