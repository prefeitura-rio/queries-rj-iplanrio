{{ config(alias='tipo_justificativa_falta', schema='brutos_gestao_escolar') }}

with source as (
    select * from {{ source('sme_brutos_gestao_escolar_staging_airbyte', 'ACA_TipoJustificativaFalta') }}
),

renamed as (
    select
        {{ adapter.quote("_airbyte_extracted_at") }} AS loaded_at,
        SAFE_CAST({{ adapter.quote("tjf_id") }} AS STRING) AS {{ adapter.quote("tjf_id") }},
        {{ adapter.quote("tjf_nome") }},
        {{ adapter.quote("tjf_situacao") }},
        {{ adapter.quote("tjf_dataCriacao") }},
        {{ adapter.quote("tjf_dataAlteracao") }},
        {{ adapter.quote("tjf_abonaFalta") }},
        {{ adapter.quote("tjf_codigo") }}
    from source
)


select * from renamed
