{{
  config(
    alias='museu_villa_lobos',
  )
}}

with dados_com_linha as (
  select 
    string_field_0 as mes_ano,
    string_field_1 as no_visitantes,
    row_number() over() as linha
  from {{ source('brutos_oferta_turistica_staging', 'museu_villa_lobos') }}
)

select 
  mes_ano,
  no_visitantes
from dados_com_linha
where linha > 1
