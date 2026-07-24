{{ config(alias='aluno_curriculo', schema='brutos_gestao_escolar') }}

with source as (
    select * from {{ source('sme_brutos_gestao_escolar_staging_airbyte', 'ACA_AlunoCurriculo') }}
),

renamed as (
    select
        {{ adapter.quote("_airbyte_extracted_at") }} AS loaded_at,
        SAFE_CAST({{ adapter.quote("alu_id") }} AS STRING) AS {{ adapter.quote("alu_id") }},
        SAFE_CAST({{ adapter.quote("alc_id") }} AS STRING) AS {{ adapter.quote("alc_id") }},
        SAFE_CAST({{ adapter.quote("esc_id") }} AS STRING) AS {{ adapter.quote("esc_id") }},
        SAFE_CAST({{ adapter.quote("uni_id") }} AS STRING) AS {{ adapter.quote("uni_id") }},
        SAFE_CAST({{ adapter.quote("cur_id") }} AS STRING) AS {{ adapter.quote("cur_id") }},
        SAFE_CAST({{ adapter.quote("crr_id") }} AS STRING) AS {{ adapter.quote("crr_id") }},
        SAFE_CAST({{ adapter.quote("crp_id") }} AS STRING) AS {{ adapter.quote("crp_id") }},
        {{ adapter.quote("alc_matricula") }},
        {{ adapter.quote("alc_codigoInep") }},
        {{ adapter.quote("alc_dataPrimeiraMatricula") }},
        {{ adapter.quote("alc_dataSaida") }},
        {{ adapter.quote("alc_dataColacao") }},
        {{ adapter.quote("alc_situacao") }},
        {{ adapter.quote("alc_dataCriacao") }},
        {{ adapter.quote("alc_dataAlteracao") }},
        {{ adapter.quote("alc_matriculaEstadual") }},
        {{ adapter.quote("alc_qtdeImpressoesHistorico") }},
        {{ adapter.quote("alc_registroGeral") }}
    from source
)


select * from renamed
