{{ config(
    materialized='table',
    schema='turismo_fluxo_visitantes',
    alias='rede_hoteleira_ocupacao_geral',
) }}

SELECT DISTINCT
  SAFE_CAST(mes_ano AS DATE FORMAT "DD/MM/YYYY") as data,
  ROUND(SAFE_CAST(REPLACE(tx_de_ocupacao_, ",", ".") AS FLOAT64)/100,4) as taxa_ocupacao
FROM {{ ref('raw_oferta_turistica__rede_hoteleira_ocupacao_geral') }}
WHERE mes_ano is not null