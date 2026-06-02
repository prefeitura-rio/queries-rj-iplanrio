{{
  config(
    materialized='table',
    alias='licenca_transferida'
  )
}}

SELECT
    CAST(CAST(num_lic_Origem AS FLOAT64) AS int64) AS id_licenciamento_origem,
    CAST(CAST(num_lic_Destino AS FLOAT64) AS int64) AS id_licenciamento_destino,
    CAST(DtTransferencia AS DATETIME) AS data_transferencia,
    CODORGAOSIGMA_ORIGEM AS id_orgao_SISLIC_origem,
    CODORGAOSIGMA_DESTINO AS id_orgao_SISLIC_destino,

FROM {{ source('adm_licenca_urbanismo_staging', 'licenca_transferida') }}
