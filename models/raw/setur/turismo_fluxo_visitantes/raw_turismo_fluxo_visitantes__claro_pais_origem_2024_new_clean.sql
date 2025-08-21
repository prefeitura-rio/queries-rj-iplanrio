{{
  config(
    alias='claro_pais_origem_2024_new_clean',
  )
}}

with dados_com_linha as (
  select 
    string_field_0 as unnamed_0,
    int64_field_1 as janeiro,
    int64_field_2 as fevereiro,
    int64_field_3 as marco,
    int64_field_4 as abril,
    int64_field_5 as maio,
    int64_field_6 as junho,
    int64_field_7 as julho,
    int64_field_8 as agosto,
    int64_field_9 as setembro,
    int64_field_10 as outubro,
    int64_field_11 as novembro,
    int64_field_12 as dezembro,
    int64_field_13 as total,
    double_field_14 as unnamed_14,
    row_number() over() as linha
  from {{ source('turismo_fluxo_visitantes_staging', 'claro_pais_origem_2024_new') }}
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
where linha > 1
