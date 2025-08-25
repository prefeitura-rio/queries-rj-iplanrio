{{ config(
    materialized='table',
    schema='turismo_fluxo_visitantes',
    alias='rede_hoteleira_ocupacao_eventos',
) }}

SELECT
  TRIM(ano) as ano,
  SAFE_CAST(data_inicial as DATE FORMAT "DD/MM/YYYY") as data_inicial,
  SAFE_CAST(data_final as DATE FORMAT "DD/MM/YYYY") as data_final,
  TRIM(evento) as evento,
  ROUND(SAFE_CAST(REPLACE(tx_de_ocupacao_, ",", ".") AS FLOAT64)/100,4) as taxa_ocupacao,
FROM {{ ref('raw_oferta_turistica__rede_hoteleira_ocupacao_grandes_eventos') }}