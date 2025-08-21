{{
  config(
    alias='rede_hoteleira_ocupacao_geral',
  )
}}

with dados_com_linha as (
  select 
    string_field_0 as mes_ano,
    string_field_1 as tx_de_ocupacao_,
    row_number() over() as linha
  from {{ source('brutos_oferta_turistica_staging', 'rede_hoteleira_ocupacao_geral') }}
)

select 
  mes_ano,
  tx_de_ocupacao_
from dados_com_linha
where linha > 1
