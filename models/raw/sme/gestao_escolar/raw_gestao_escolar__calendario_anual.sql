{{ config(alias='calendario_anual', schema='brutos_gestao_escolar') }}

with source as (
    select * from {{ source('brutos_gestao_escolar_staging_airbyte', 'ACA_CalendarioAnual') }}
),

renamed as (
    select
        {{ adapter.quote("_airbyte_extracted_at") }} AS loaded_at,
        SAFE_CAST({{ adapter.quote("cal_id") }} AS STRING) AS {{ adapter.quote("cal_id") }},
        SAFE_CAST({{ adapter.quote("ent_id") }} AS STRING) AS {{ adapter.quote("ent_id") }},
        {{ adapter.quote("cal_padrao") }},
        {{ adapter.quote("cal_ano") }},
        {{ adapter.quote("cal_descricao") }},
        {{ adapter.quote("cal_dataInicio") }},
        {{ adapter.quote("cal_dataFim") }},
        {{ adapter.quote("cal_situacao") }},
        {{ adapter.quote("cal_dataCriacao") }},
        {{ adapter.quote("cal_dataAlteracao") }}
    from source
)


select * from renamed
