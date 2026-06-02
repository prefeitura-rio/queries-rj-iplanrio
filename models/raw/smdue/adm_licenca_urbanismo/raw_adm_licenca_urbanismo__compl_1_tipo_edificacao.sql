{{
  config(
    materialized='table',
    alias='compl_1_tipo_edificacao'
  )
}}

SELECT
Cod_TpEdificacao AS id_tipo_edificacao,
Cod_Compl_TpEdificacao AS id_compl_tipo_edificacao,
Compl_TpEdificacao AS nome_tipo_edificacao,

FROM {{ source('adm_licenca_urbanismo_staging', 'compl_1_tipo_edificacao') }}