{{ config(alias='processo_fechamento_inicio') }}

with source as (
    select * from {{ source('sme_brutos_gestao_escolar_staging_airbyte', 'MTR_ProcessoFechamentoInicio') }}
),

renamed as (
    select
        {{ adapter.quote("_airbyte_extracted_at") }} AS loaded_at,
        SAFE_CAST({{ adapter.quote("pfi_id") }} AS STRING) AS {{ adapter.quote("pfi_id") }},
        {{ adapter.quote("pfi_anoFechamento") }},
        {{ adapter.quote("pfi_anoInicio") }},
        {{ adapter.quote("pfi_remanejamentoNaoAtendido") }},
        {{ adapter.quote("pfi_situacao") }},
        {{ adapter.quote("pfi_dataCriacao") }},
        {{ adapter.quote("pfi_dataAlteracao") }},
        SAFE_CAST({{ adapter.quote("ent_id") }} AS STRING) AS {{ adapter.quote("ent_id") }},
        {{ adapter.quote("pfi_dataReferencia") }},
        {{ adapter.quote("pfi_anoLetivoCorrente") }},
        {{ adapter.quote("pfi_dataEncerramentoRegencia") }},
        {{ adapter.quote("pfi_dataEncVinculoComplementacao") }},
        {{ adapter.quote("pfi_dataEncVinculoDuplaRegencia") }},
        {{ adapter.quote("pfi_dataRenovacao") }},
        {{ adapter.quote("pfi_dataReferenciaEdInfantil") }}
    from source
)


select * from renamed
