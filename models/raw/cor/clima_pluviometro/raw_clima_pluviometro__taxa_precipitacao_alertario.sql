{{
    config(
        alias="taxa_precipitacao_alertario",
        materialized='table',
        unique_key="primary_key",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "month",
        },
        post_hook='CREATE OR REPLACE TABLE `rj-cor.clima_pluviometro_staging.taxa_precipitacao_alertario_last_partition_datario` AS (SELECT CURRENT_DATE("America/Sao_Paulo") AS data)'
    )
}}

SELECT
    CONCAT(id_estacao, '_', data_medicao) AS primary_key,
    SAFE_CAST(id_estacao AS STRING) AS id_estacao,
    SAFE_CAST(data_medicao AS DATETIME) AS data_medicao,
    SAFE_CAST(acumulado_chuva_5min AS FLOAT64) AS acumulado_chuva_5min,
    SAFE_CAST(acumulado_chuva_10min AS FLOAT64) AS acumulado_chuva_10min,
    SAFE_CAST(acumulado_chuva_15min AS FLOAT64) AS acumulado_chuva_15min,
    SAFE_CAST(acumulado_chuva_30min AS FLOAT64) AS acumulado_chuva_30min,
    SAFE_CAST(acumulado_chuva_1h AS FLOAT64) AS acumulado_chuva_1h,
    SAFE_CAST(acumulado_chuva_2h AS FLOAT64) AS acumulado_chuva_2h,
    SAFE_CAST(acumulado_chuva_3h AS FLOAT64) AS acumulado_chuva_3h,
    SAFE_CAST(acumulado_chuva_4h AS FLOAT64) AS acumulado_chuva_4h,
    SAFE_CAST(acumulado_chuva_6h AS FLOAT64) AS acumulado_chuva_6h,
    SAFE_CAST(acumulado_chuva_12h AS FLOAT64) AS acumulado_chuva_12h,
    SAFE_CAST(acumulado_chuva_24h AS FLOAT64) AS acumulado_chuva_24h,
    SAFE_CAST(acumulado_chuva_96h AS FLOAT64) AS acumulado_chuva_96h,
    SAFE_CAST(acumulado_chuva_mes AS FLOAT64) AS acumulado_chuva_mes,
    SAFE_CAST(ano AS INT64) AS ano,
    SAFE_CAST(mes AS INT64) AS mes,
    SAFE_CAST(data AS DATE) AS data
FROM {{ source('clima_pluviometro_staging', 'taxa_precipitacao_alertario') }}


{% if is_incremental() %}

{% set max_partition = run_query(
    "SELECT DATE(gr) FROM (
        SELECT IF(
            max(data) > CURRENT_DATE('America/Sao_Paulo'), CURRENT_DATE('America/Sao_Paulo'), max(data)
            ) as gr 
        FROM `rj-cor.clima_pluviometro_staging.taxa_precipitacao_alertario_last_partition_datario`
        )
    ").columns[0].values()[0] %}

WHERE
    data >= ("{{ max_partition }}")

{% endif %}