{{
  config(
    alias='claro_atrativos_turistas_2025_clean',
  )
}}

with dados_com_linha as (
  select 
    string_field_0 as _2023,
    string_field_1 as cristo_redentor,
    string_field_2 as pao_de_acucar,
    string_field_3 as selaron,
    string_field_4 as ccbb_correios,
    string_field_5 as praia_de_copacabana_leme,
    string_field_6 as jardim_botanico,
    string_field_7 as catedral_metropolitana,
    string_field_8 as lapa_bairro,
    string_field_9 as boulevard_olimpico,
    string_field_10 as lagoa_rodrigo_de_freitas,
    string_field_11 as floresta_da_tijuca,
    string_field_12 as maracana,
    string_field_13 as pequena_africa,
    row_number() over() as linha
  from {{ source('turismo_fluxo_visitantes_staging', 'claro_atrativos_turistas_2025') }}
)

select 
  _2023,
  cristo_redentor,    
  pao_de_acucar,
  selaron,
  ccbb_correios,
  praia_de_copacabana_leme,
  jardim_botanico,
  catedral_metropolitana,
  lapa_bairro,
  boulevard_olimpico,
  lagoa_rodrigo_de_freitas,
  floresta_da_tijuca,
  maracana,
  pequena_africa
from dados_com_linha
where linha > 1
