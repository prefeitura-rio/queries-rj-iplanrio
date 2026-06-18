{{
    config(
        alias='taxa_precipitacao_alertario_5min',
        schema='clima_pluviometro',
        materialized='incremental',
        unique_key="primary_key",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

SELECT
    DISTINCT
    CONCAT(id_estacao, '_', data_medicao) AS primary_key,
    SAFE_CAST(id_estacao AS STRING) AS id_estacao,
    SAFE_CAST(data_medicao AS DATETIME) data_medicao,
    SAFE_CAST(SPLIT(data_medicao, " ")[0] AS DATE) data_particao,
    SAFE_CAST(SPLIT(data_medicao, " ")[1] AS TIME) horario,
    SAFE_CAST(acumulado_chuva_5min AS FLOAT64) acumulado_chuva_5min,
    SAFE_CAST(acumulado_chuva_10min AS FLOAT64) acumulado_chuva_10min,
    SAFE_CAST(acumulado_chuva_15min AS FLOAT64) acumulado_chuva_15min,
    SAFE_CAST(acumulado_chuva_30min AS FLOAT64) acumulado_chuva_30min,
    SAFE_CAST(acumulado_chuva_1h AS FLOAT64) acumulado_chuva_1h,
    SAFE_CAST(acumulado_chuva_2h AS FLOAT64) acumulado_chuva_2h,
    SAFE_CAST(acumulado_chuva_3h AS FLOAT64) acumulado_chuva_3h,
    SAFE_CAST(acumulado_chuva_4h AS FLOAT64) acumulado_chuva_4h,
    SAFE_CAST(acumulado_chuva_6h AS FLOAT64) acumulado_chuva_6h,
    SAFE_CAST(acumulado_chuva_12h AS FLOAT64) acumulado_chuva_12h,
    SAFE_CAST(acumulado_chuva_24h AS FLOAT64) acumulado_chuva_24h,
    SAFE_CAST(acumulado_chuva_96h AS FLOAT64) acumulado_chuva_96h,
    SAFE_CAST(acumulado_chuva_mes AS FLOAT64) acumulado_chuva_mes,

FROM `rj-iplanrio.clima_pluviometro_staging.taxa_precipitacao_alertario_5min`

{% if is_incremental() %}

where DATETIME(data_medicao) > (SELECT MAX(DATETIME(data_medicao)) FROM {{ this }} where DATE(data_medicao) != '2099-03-03')

{% endif %}