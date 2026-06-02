{{
  config(
    materialized='table',
    alias='tipo_unidade'
  )
}}

SELECT
    Cod_Unidade AS id_unidade,
    Desc_Unidade AS nome_unidade,
    Desc_unidade_Plural AS nome_unidade_plural

FROM {{ source('adm_licenca_urbanismo_staging', 'tipo_unidade') }}
