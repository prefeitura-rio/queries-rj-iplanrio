{{
    config(
        alias="taxa_precipitacao_alertario_5min",
        incremental_strategy='merge',
        materialized='incremental',
        unique_key="primary_key",
        partition_by={
            "field": "data_medicao",
            "data_type": "datetime",
            "granularity": "day",
        },
        cluster_by=[
            "primary_key",
            "id_estacao"
        ]
    )
}}

with source_1 as (
    SELECT DISTINCT
        CONCAT(id_estacao, '_', data_medicao) AS primary_key,
        SAFE_CAST(id_estacao AS STRING) AS id_estacao,
        SAFE_CAST(data_medicao AS DATETIME) AS data_medicao,
        TIME(SAFE_CAST(data_medicao AS DATETIME)) AS horario,
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
        SAFE_CAST(ano_particao AS INT64) AS ano_particao,
        SAFE_CAST(mes_particao AS INT64) AS mes_particao,
        SAFE_CAST(data_particao AS DATE) AS data_particao
    FROM {{ source('clima_pluviometro_staging', 'taxa_precipitacao_alertario_5min') }}

    {% if is_incremental() %}
    WHERE DATETIME(data_medicao) > (
        SELECT MAX(DATETIME(data_medicao))
        FROM {{ this }}
        WHERE DATE(data_medicao) != '2099-03-03'
    )
    {% endif %}
)

{% if not is_incremental() %}

, source_2 as (
    SELECT DISTINCT
        CONCAT(id_estacao, '_', data_medicao) AS primary_key,
        SAFE_CAST(id_estacao AS STRING) AS id_estacao,
        SAFE_CAST(data_medicao AS DATETIME) AS data_medicao,
        TIME(SAFE_CAST(data_medicao AS DATETIME)) AS horario,
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
        SAFE_CAST(ano_particao AS INT64) AS ano_particao,
        SAFE_CAST(mes_particao AS INT64) AS mes_particao,
        SAFE_CAST(data_particao AS DATE) AS data_particao
    FROM `rj-cor.clima_pluviometro_staging.taxa_precipitacao_alertario_5min`
    WHERE data_particao BETWEEN '2025-01-01' AND '2026-05-31'
)

{% endif %}

SELECT * FROM source_1

{% if not is_incremental() %}
UNION ALL
SELECT * FROM source_2
{% endif %}