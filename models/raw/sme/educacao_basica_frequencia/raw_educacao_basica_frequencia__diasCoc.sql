{{ config(
    alias='diasCoc',
    materialized='view',
) }}


with source as (
    select * from {{ source('sme_brutos_educacao_basica_frequencia_staging_prefect', 'diasCoc') }}
  ),
  renamed as (
      select
        {{ adapter.quote("cal_id") }},
        {{ adapter.quote("tpc_id") }},
        CAST({{ adapter.quote("diascoc") }} AS INT64) AS {{ adapter.quote("diasCoc") }},

      from source
  )
  select * from renamed
