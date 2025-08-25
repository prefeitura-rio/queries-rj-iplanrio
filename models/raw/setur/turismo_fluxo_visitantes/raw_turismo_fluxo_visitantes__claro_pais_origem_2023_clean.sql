{{
  config(
    alias='claro_pais_origem_2023_clean',
  )
}}

with dados_com_linha as (
  select 
    string_field_0 as unnamed_0,
    string_field_1 as janeiro,
    string_field_2 as fevereiro,
    string_field_3 as marco,
    string_field_4 as abril,
    string_field_5 as maio,
    string_field_6 as junho,
    string_field_7 as julho,
    string_field_8 as agosto,
    string_field_9 as setembro,
    string_field_10 as outubro,
    string_field_11 as novembro,
    string_field_12 as dezembro,
    string_field_13 as total,
    string_field_14 as unnamed_14,
    row_number() over() as linha
  from {{ source('turismo_fluxo_visitantes_staging', 'claro_pais_origem_new') }}
)

select 
  unnamed_0,
  janeiro,  
  fevereiro,
  marco,
  abril,
  maio,
  junho,
  julho,
  agosto,
  setembro,
  outubro,
  novembro,
  dezembro,
  total,
  unnamed_14
from dados_com_linha
