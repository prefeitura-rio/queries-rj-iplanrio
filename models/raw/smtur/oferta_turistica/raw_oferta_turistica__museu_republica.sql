{{
  config(
    alias='museu_republica',
    materialized='view'
  )
}}

with dados_com_linha as (
  select 
    string_field_0 as mes_ano,
    string_field_1 as numero_visitantes,
    row_number() over() as linha
  from {{ source('oferta_turistica', 'museu_republica') }}
)

select 
  mes_ano,
  numero_visitantes
from dados_com_linha
where linha > 1
