{{
    config(
        alias="qmd_plano",
        schema="brutos_forca_municipal",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
        cluster_by=["id_responsavel_criacao"],
    )
}}

with
    source as (
        select *
        from {{ source('brutos_forca_municipal_staging', 'qmd_plano') }}
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

            -- dados
            {{ padronize_id('Id') }} as id_plano,
            {{ proper_br('safe_cast(Nome as string)') }} as nome,
            datetime(
                safe_cast(SemanaReferenciaInicio as timestamp), 'America/Sao_Paulo'
            ) as data_hora_semana_referencia_inicio,
            datetime(
                safe_cast(SemanaReferenciaFim as timestamp), 'America/Sao_Paulo'
            ) as data_hora_semana_referencia_fim,
            datetime(
                safe_cast(regexp_replace(DataHoraCriacao, r'(\.\d{6})\d+', r'\1') as timestamp),
                'America/Sao_Paulo'
            ) as data_hora_criacao,

            -- dados derivados do nome (break estrutural 2026-05-03: unificado → 3 bases)
            case
                when Nome like '%Norte%'     then 'NORTE'
                when Nome like '%Oeste%'     then 'OESTE'
                when Nome like '%Litorânea%' then 'LITORANEA'
                else null
            end as area,
            safe_cast(
                regexp_extract(Nome, r'- Semana (\d+)') as int64
            ) as numero_semana,
            regexp_contains(Nome, r'Força Municipal') as indicador_plano_unificado,
            regexp_contains(Nome, r'Encerrado')       as indicador_plano_encerrado,
            regexp_contains(lower(Nome), r'\(teste\)') as indicador_plano_teste,

            -- partição
            safe_cast(data_particao as date) as data_particao

        from source
    ),

    com_datas as (
        select
            *,
            date(data_hora_semana_referencia_inicio) as data_semana_referencia_inicio,
            date(data_hora_semana_referencia_fim)    as data_semana_referencia_fim
        from renamed
    )

select *
from com_datas
