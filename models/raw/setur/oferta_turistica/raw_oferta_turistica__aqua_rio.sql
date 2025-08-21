{{
  config(
    alias='aquario',
    materialized='view'
  )
}}

with dados_com_linha as (
  select 
    string_field_0 as mes_ano,
    string_field_1 as no_visitantes,
    row_number() over() as linha
  from {{ source('oferta_turistica', 'aqua_rio') }}
)

select 
  mes_ano,
  no_visitantes
from dados_com_linha
where linha > 1
