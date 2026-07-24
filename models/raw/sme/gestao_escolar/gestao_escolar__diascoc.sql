{{ config(
    alias='diascoc',
    schema='gestao_escolar',
    materialized='table',
) }}


with source as (
    select * from {{ source('sme_brutos_gestao_escolar_staging_prefect', 'diasCoc') }}
  ),
  renamed as (
      select
        {{ adapter.quote("cal_id") }},
        {{ adapter.quote("tpc_id") }},
        CAST({{ adapter.quote("diascoc") }} AS INT64) AS {{ adapter.quote("diasCoc") }},

      from source
  )
  select * from renamed
