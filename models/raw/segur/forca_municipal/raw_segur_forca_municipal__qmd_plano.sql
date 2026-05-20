{{
    config(
        alias="qmd_plano",
        schema="brutos_forca_municipal",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id_hash",
        merge_update_columns=["last_seen", "data_particao", "updated_at"],
        cluster_by=["id_responsavel_criacao"],
    )
}}

with
    source as (
        select *
        from {{ source('brutos_forca_municipal_staging', 'qmd_plano') }}
        {% if is_incremental() %}
            -- runs incrementais: só a última partição disponível
            where safe_cast(data_particao as date) = (
                select max(safe_cast(data_particao as date))
                from {{ source('brutos_forca_municipal_staging', 'qmd_plano') }}
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
            regexp_contains(lower(Nome), r'\(teste\)') as indicador_plano_teste

        from source
    ),

    com_datas as (
        select
            *,
            date(data_hora_semana_referencia_inicio) as data_semana_referencia_inicio,
            date(data_hora_semana_referencia_fim)    as data_semana_referencia_fim
        from renamed
    ),

    deduplicado as (
        select
            -- metadados da pipeline
            id_hash,
            min(updated_at) as first_seen,
            max(updated_at) as last_seen,
            max(updated_at)    as updated_at,
            max(data_particao) as data_particao,

            -- identificadores
            any_value(id_responsavel_criacao) as id_responsavel_criacao,

            -- dados
            any_value(id_plano)                          as id_plano,
            any_value(nome)                              as nome,
            any_value(data_hora_semana_referencia_inicio) as data_hora_semana_referencia_inicio,
            any_value(data_hora_semana_referencia_fim)    as data_hora_semana_referencia_fim,
            any_value(data_hora_criacao)                  as data_hora_criacao,
            any_value(area)                              as area,
            any_value(numero_semana)                     as numero_semana,
            any_value(indicador_plano_unificado)         as indicador_plano_unificado,
            any_value(indicador_plano_encerrado)         as indicador_plano_encerrado,
            any_value(indicador_plano_teste)             as indicador_plano_teste,
            any_value(data_semana_referencia_inicio)     as data_semana_referencia_inicio,
            any_value(data_semana_referencia_fim)        as data_semana_referencia_fim
        from com_datas
        group by id_hash
    )

select *
from deduplicado
