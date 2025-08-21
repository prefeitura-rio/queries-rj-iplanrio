{{
  config(
    alias='galeao',
    materialized='view'
  )
}}

with dados_com_linha as (
  select 
    string_field_0 as mes_ano,
    string_field_1 as total_de_passageiros,
    string_field_2 as passageiros_domesticos,
    string_field_3 as passageiros_internacionais,
    row_number() over() as linha
  from {{ source('oferta_turistica', 'galeao') }}
)

select 
  mes_ano,
  total_de_passageiros,
  passageiros_domesticos,
  passageiros_internacionais
from dados_com_linha
where linha > 1
