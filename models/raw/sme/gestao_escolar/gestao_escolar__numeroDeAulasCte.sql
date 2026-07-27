{{ config(
    alias='numeroDeAulasCte',
    schema='gestao_escolar',
    materialized='ephemeral',
) }}


with source as (
    select * from {{ source('brutos_gestao_escolar_staging', 'numeroDeAulasCte') }}
  ),
  renamed as (
      select
        {{ adapter.quote("alu_id") }},
        {{ adapter.quote("mtu_id") }},
        {{ adapter.quote("tpc_id") }},
        CAST({{ adapter.quote("numeroAulas") }} AS INT64) AS numeroAulas,

      from source
  )
  select * from renamed
