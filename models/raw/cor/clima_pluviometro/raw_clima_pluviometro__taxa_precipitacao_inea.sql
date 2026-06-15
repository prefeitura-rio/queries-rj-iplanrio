{{
    config(
        alias="taxa_precipitacao_inea",
        materialized='table',
        unique_key="primary_key",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "month",
        },
        post_hook='CREATE OR REPLACE TABLE `rj-cor.clima_pluviometro_staging.taxa_precipitacao_inea_last_partition_datario` AS (SELECT CURRENT_DATE("America/Sao_Paulo") AS data)'
    )
}}

SELECT
    CONCAT(id_estacao, '_', data_medicao) AS primary_key,
    SAFE_CAST(id_estacao AS STRING) AS id_estacao,
    SAFE_CAST(data_medicao AS DATETIME) AS data_medicao,
    SAFE_CAST(acumulado_chuva_15_min AS FLOAT64) AS acumulado_chuva_15_min,
    SAFE_CAST(acumulado_chuva_1_h AS FLOAT64) AS acumulado_chuva_1_h,
    SAFE_CAST(acumulado_chuva_4_h AS FLOAT64) AS acumulado_chuva_4_h,
    SAFE_CAST(acumulado_chuva_24_h AS FLOAT64) AS acumulado_chuva_24_h,
    SAFE_CAST(acumulado_chuva_96_h AS FLOAT64) AS acumulado_chuva_96_h,
    SAFE_CAST(acumulado_chuva_30_d AS FLOAT64) AS acumulado_chuva_30_d,
    SAFE_CAST(ano AS INT64) AS ano,
    SAFE_CAST(mes AS INT64) AS mes,
    SAFE_CAST(data AS DATE) AS data
FROM {{ source('clima_pluviometro_staging', 'taxa_precipitacao_inea') }}


{% if is_incremental() %}

{% set max_partition = run_query(
    "SELECT DATE(gr) FROM (
        SELECT IF(
            max(data) > CURRENT_DATE('America/Sao_Paulo'), CURRENT_DATE('America/Sao_Paulo'), max(data)
            ) as gr 
        FROM `rj-cor.clima_pluviometro_staging.taxa_precipitacao_inea_last_partition_datario`
        )
    ").columns[0].values()[0] %}

WHERE
    data >= ("{{ max_partition }}")

{% endif %}