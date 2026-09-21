{{ config(alias="calendario_periodo", schema="gestao_escolar") }}

with
    source as (
        select *
        from {{ source("brutos_gestao_escolar_staging", "ACA_CalendarioPeriodo") }}
    ),

    renamed as (
        select
            {{ adapter.quote("_airbyte_extracted_at") }} as loaded_at,
            safe_cast(
                {{ adapter.quote("cal_id") }} as string
            ) as {{ adapter.quote("cal_id") }},
            safe_cast(
                {{ adapter.quote("cap_id") }} as string
            ) as {{ adapter.quote("cap_id") }},
            {{ adapter.quote("cap_descricao") }},
            safe_cast(
                {{ adapter.quote("tpc_id") }} as string
            ) as {{ adapter.quote("tpc_id") }},
            {{ adapter.quote("cap_dataInicio") }},
            {{ adapter.quote("cap_dataFim") }},
            {{ adapter.quote("cap_situacao") }},
            {{ adapter.quote("cap_dataCriacao") }},
            {{ adapter.quote("cap_dataAlteracao") }}
        from source
    ),

    final as (
        select {{ dbt_utils.generate_surrogate_key(["cal_id", "tpc_id"]) }} as id, *
        from renamed
    ),

    dedup as (
        select *
        from
            final 
            qualify 
                row_number() over (
                    partition by id order by cap_dataalteracao desc
                ) = 1
    )

select *
from dedup
