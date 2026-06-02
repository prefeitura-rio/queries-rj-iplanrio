{{
  config(
    materialized='table',
    alias='adm_licenca_urbanismo__area_protecao_processo'
  )
}}

SELECT
ID_AREA_GRAU AS id_area_grau,
ID_AREA_PROTECAO_CULTURAL AS id_area_protecao_cultural,
ID_GRAU_PROTECAO AS id_grau_protecao,
ANTERIOR_1938 AS anterior_1938,
SITIO_RIO_PATRIMONIO_MUNDIAL AS sitio_rio_patrimonio_mundial,

FROM {{ source('adm_licenca_urbanismo_staging', 'area_protecao_processo') }}
