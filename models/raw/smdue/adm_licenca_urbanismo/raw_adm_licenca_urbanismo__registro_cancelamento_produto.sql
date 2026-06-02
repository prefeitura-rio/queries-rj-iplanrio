{{
  config(
    materialized='table',
    alias='registro_cancelamento_produto'
  )
}}

SELECT
    cod_dlf AS id_orgao_SISLIC,
    tipo AS tipo,
    Nr_documento AS numero_documento,
    CAST(dt_cancelamento AS DATETIME) AS data_cancelamento

FROM {{ source('adm_licenca_urbanismo_staging', 'registro_cancelamento_produto') }}
