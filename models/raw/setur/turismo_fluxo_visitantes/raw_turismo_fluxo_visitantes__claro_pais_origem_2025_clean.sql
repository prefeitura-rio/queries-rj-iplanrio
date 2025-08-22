{{
  config(
    alias='claro_pais_origem_2025_clean',
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
  from {{ source('turismo_fluxo_visitantes_staging', 'claro_pais_origem_2025') }}
)

select 
  unnamed_0,
  REPLACE(janeiro, '.', '') as janeiro,
  REPLACE(fevereiro, '.', '') as fevereiro,
  REPLACE(marco, '.', '') as marco,
  REPLACE(abril, '.', '') as abril,
  REPLACE(maio, '.', '') as maio,
  REPLACE(junho, '.', '') as junho,
  REPLACE(julho, '.', '') as julho,
  REPLACE(agosto, '.', '') as agosto,
  REPLACE(setembro, '.', '') as setembro,
  REPLACE(outubro, '.', '') as outubro,
  REPLACE(novembro, '.', '') as novembro,
  REPLACE(dezembro, '.', '') as dezembro,
  total,
  unnamed_14
from dados_com_linha
where linha > 1
