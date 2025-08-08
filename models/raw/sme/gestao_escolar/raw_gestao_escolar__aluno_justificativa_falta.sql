{{ config(alias='aluno_justificativa_falta', schema='brutos_gestao_escolar') }}

with source as (
    select * from {{ source('brutos_gestao_escolar_staging_airbyte', 'ACA_AlunoJustificativaFalta') }}
),

renamed as (
    select
        {{ adapter.quote("_airbyte_extracted_at") }} AS loaded_at,
        SAFE_CAST({{ adapter.quote("alu_id") }} AS STRING) AS {{ adapter.quote("alu_id") }},
        SAFE_CAST({{ adapter.quote("afj_id") }} AS STRING) AS {{ adapter.quote("afj_id") }},
        SAFE_CAST({{ adapter.quote("tjf_id") }} AS STRING) AS {{ adapter.quote("tjf_id") }},
        {{ adapter.quote("afj_dataInicio") }},
        {{ adapter.quote("afj_dataFim") }},
        {{ adapter.quote("afj_situacao") }},
        {{ adapter.quote("afj_dataCriacao") }},
        {{ adapter.quote("afj_dataAlteracao") }},
        SAFE_CAST({{ adapter.quote("pro_id") }} AS STRING) AS {{ adapter.quote("pro_id") }}
    from source
)


select * from renamed
