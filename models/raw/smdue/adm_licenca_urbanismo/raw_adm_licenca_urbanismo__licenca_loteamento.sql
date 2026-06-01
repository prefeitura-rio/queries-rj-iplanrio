{{
  config(
    materialized='table',
    alias='licenca_loteamento'
  )
}}

SELECT
CAST(Id_Lote  AS string) AS id_lote ,
CAST(num_lic AS string) AS id_licenciamento,
CAST(CAST(Quantidade AS FLOAT64)  AS int64) AS quantidade_lotes,
CAST(CAST(Categoria AS FLOAT64)  AS int64) AS categoria_lote,
CAST(CAST(TpLote AS FLOAT64)  AS int64) AS lote_aprovado,

FROM {{ source('adm_licenca_urbanismo_staging', 'licenca_loteamento') }}
