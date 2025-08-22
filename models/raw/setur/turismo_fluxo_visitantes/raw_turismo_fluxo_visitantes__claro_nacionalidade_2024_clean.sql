{{
  config(
    alias='claro_nacionalidade_2024_clean',
  )
}}

with dados_com_linha as (
  select 
    string_field_0 as _2023,
    string_field_1 as internacionais,
    string_field_2 as nacionais,
    string_field_3 as total,
    row_number() over() as linha
  from {{ source('turismo_fluxo_visitantes_staging', 'claro_nacionalidade_2024_new') }}
)

select 
  _2023,
  internacionais,
  nacionais,
  total
from dados_com_linha
where linha > 1
