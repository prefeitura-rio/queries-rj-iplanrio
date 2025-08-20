{{
  config(
    alias='iss',
    materialized='view'
  )
}}

with dados_com_linha as (
  select 
    string_field_0 as mes_ano,
    string_field_1 as total_arrecadado,
    row_number() over() as linha
  from {{ source('oferta_turistica', 'iss') }}
)

select 
  mes_ano,
  total_arrecadado
from dados_com_linha
where linha > 1
