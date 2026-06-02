{{
  config(
    materialized='table',
    alias='adm_licenca_urbanismo__area_protecao_cultural'
  )
}}

SELECT
ID_AREA_PROTECAO_CULTURAL AS id_area_protecao_cultural,
REGEXP_REPLACE(
    NORMALIZE(UPPER(DS_AREA_PROTECAO_CULTURAL), NFD),
    r'\p{M}',
    ''
  ) AS nome_area_protecao_ambiental,

FROM {{ source('adm_licenca_urbanismo_staging', 'area_protecao_cultural') }}
