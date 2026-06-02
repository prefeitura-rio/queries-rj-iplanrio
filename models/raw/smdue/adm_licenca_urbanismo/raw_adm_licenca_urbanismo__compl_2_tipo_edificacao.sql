{{
  config(
    materialized='table',
    alias='compl_2_tipo_edificacao'
  )
}}

SELECT
    cod_Edif AS id_tipo_edificacao,
    cod_compl_TpEdificacao AS id_compl_tipo_edificacao,
    cod_compl_tpEdificacao2 AS id_compl_tipo_edificacao_2,
    Compl2 AS nome_tipo_edificacao,

FROM {{ source('adm_licenca_urbanismo_staging', 'compl_2_tipo_edificacao') }}
