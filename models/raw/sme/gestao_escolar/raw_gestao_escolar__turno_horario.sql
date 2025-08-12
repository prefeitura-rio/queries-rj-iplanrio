{{ config(alias='turno_horario', schema='brutos_gestao_escolar') }}

with source as (
    select * from {{ source('sme_brutos_gestao_escolar_staging_airbyte', 'ACA_TurnoHorario') }}
),

renamed as (
    select
        {{ adapter.quote("_airbyte_extracted_at") }} AS loaded_at,
            SAFE_CAST({{ adapter.quote("trn_id") }} AS STRING) AS {{ adapter.quote("trn_id") }},
            SAFE_CAST({{ adapter.quote("trh_id") }} AS STRING) AS {{ adapter.quote("trh_id") }},
            {{ adapter.quote("trh_diaSemana") }},
            {{ adapter.quote("trh_horaInicio") }},
            {{ adapter.quote("trh_horaFim") }},
            {{ adapter.quote("trh_tipo") }},
            {{ adapter.quote("trh_situacao") }},
            {{ adapter.quote("trh_dataCriacao") }},
            {{ adapter.quote("trh_dataAlteracao") }}
    from source
)


select * from renamed
