{{ config(
    materialized='table',
    schema='turismo_fluxo_visitantes',
    alias='rodoviarias',
) }}

SELECT
    SAFE_CAST(mes_ano AS DATE FORMAT "DD/MM/YYYY") as data,
    "Novo Rio" as nome_rodoviaria,
    SAFE_CAST(SAFE_CAST(total_de_passageiros as FLOAT64) as INT64) as passageiros_total,
FROM {{ ref('raw_oferta_turistica__novo_rio') }}
WHERE SAFE_CAST(mes_ano AS DATE FORMAT "DD/MM/YYYY") <= CURRENT_DATE()