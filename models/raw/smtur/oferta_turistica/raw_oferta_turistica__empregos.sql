{{
  config(
    alias='empregos',
    materialized='view'
  )
}}

with dados_com_linha as (
  select 
    string_field_0 as mes_ano,
    string_field_1 as saldo,
    row_number() over() as linha
  from {{ source('oferta_turistica', 'empregos') }}
)

select 
  mes_ano,
  saldo
from dados_com_linha
where linha > 1
