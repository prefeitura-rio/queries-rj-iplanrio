{{
  config(
    materialized='table',
    alias='compl_tipo_licenca_acao_requerente'
  )
}}

SELECT
    COD_LIC AS id_licenca,
    COD_COMPL_LIC AS id_complemento_tipo_licenca,
    COMPL AS complemento,
    GRATIS AS gratis

FROM {{ source('adm_licenca_urbanismo_staging', 'compl_tipo_licenca_acao_requerente') }}
