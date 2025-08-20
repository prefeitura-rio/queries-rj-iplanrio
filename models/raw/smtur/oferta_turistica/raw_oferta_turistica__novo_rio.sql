{{
  config(
    alias='novo_rio',
    materialized='view'
  )
}}

with dados_com_linha as (
  select 
    string_field_0 as mes_ano,
    string_field_1 as total_de_passageiros,
    row_number() over() as linha
  from {{ source('oferta_turistica', 'novo_rio') }}
)

select 
  mes_ano,
  total_de_passageiros
from dados_com_linha
where linha > 1
