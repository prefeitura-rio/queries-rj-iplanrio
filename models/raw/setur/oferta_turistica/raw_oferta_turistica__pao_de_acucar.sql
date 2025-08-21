{{
  config(
    alias='pao_de_acucar',
    materialized='view'
  )
}}

with dados_com_linha as (
  select 
    string_field_0 as mes_ano,
    string_field_1 as visitantes,
    row_number() over() as linha
  from {{ source('oferta_turistica', 'pao_de_acucar') }}
)

select 
  mes_ano,
  visitantes
from dados_com_linha
where linha > 1
