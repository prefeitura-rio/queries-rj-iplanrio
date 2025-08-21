{{
  config(
    alias='rede_hoteleira_ocupacao_grandes_eventos',
    materialized='view'
  )
}}

with dados_com_linha as (
  select 
    string_field_0 as ano,
    string_field_1 as evento,
    string_field_2 as tx_de_ocupacao_,
    string_field_3 as data_inicial,
    string_field_4 as data_final,

    row_number() over() as linha
  from {{ source('oferta_turistica', 'rede_hoteleira_ocupacao_grandes_eventos') }}
)

select 
  ano,
  evento,
  taxa_de_ocupacao,
  data_inicial,
  data_final,

from dados_com_linha
where linha > 1
