{{
  config(
    alias='rede_hoteleira_ocupacao_geral',
    materialized='view'
  )
}}

with dados_com_linha as (
  select 
    string_field_0 as mes_ano,
    string_field_1 as taxa_de_ocupacao,
    row_number() over() as linha
  from {{ source('oferta_turistica', 'rede_hoteleira_ocupacao_geral') }}
)

select 
  mes_ano,
  taxa_de_ocupacao
from dados_com_linha
where linha > 1
