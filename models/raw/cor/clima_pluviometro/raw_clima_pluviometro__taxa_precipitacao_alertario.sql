{{
    config(
        alias="taxa_precipitacao_alertario_final",
        materialized='incremental',
        incremental_strategy = "merge",
        unique_key="primary_key",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

with source_1 as (
    SELECT DISTINCT
        CONCAT(id_estacao, '_', data_medicao) AS primary_key,
        SAFE_CAST(id_estacao AS STRING) AS id_estacao,
        SAFE_CAST(data_medicao AS DATETIME) AS data_medicao,
        TIME(SAFE_CAST(data_medicao AS DATETIME)) AS horario,
        SAFE_CAST(acumulado_chuva_15min AS FLOAT64) AS acumulado_chuva_15_min,
        SAFE_CAST(acumulado_chuva_1h AS FLOAT64) AS acumulado_chuva_1h,
        SAFE_CAST(acumulado_chuva_4h AS FLOAT64) AS acumulado_chuva_4h,
        SAFE_CAST(acumulado_chuva_24h AS FLOAT64) AS acumulado_chuva_24h,
        SAFE_CAST(acumulado_chuva_96h AS FLOAT64) AS acumulado_chuva_96h,
        SAFE_CAST(ano_particao AS INT64) AS ano_particao,
        SAFE_CAST(mes_particao AS INT64) AS mes_particao,
        SAFE_CAST(data_particao AS DATE) AS data_particao
    FROM {{ source('clima_pluviometro_staging', 'taxa_precipitacao_alertario') }}

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
        SAFE_CAST(acumulado_chuva_15_min AS FLOAT64) AS acumulado_chuva_15_min,
        SAFE_CAST(acumulado_chuva_1_h AS FLOAT64) AS acumulado_chuva_1h,
        SAFE_CAST(acumulado_chuva_4_h AS FLOAT64) AS acumulado_chuva_4h,
        SAFE_CAST(acumulado_chuva_24_h AS FLOAT64) AS acumulado_chuva_24h,
        SAFE_CAST(acumulado_chuva_96_h AS FLOAT64) AS acumulado_chuva_96h,
        SAFE_CAST(ano AS INT64) AS ano_particao,
        SAFE_CAST(mes AS INT64) AS mes_particao,
        DATE_TRUNC(SAFE_CAST(data_medicao AS DATE), DAY) AS data_particao
    FROM `rj-cor.clima_pluviometro_staging.taxa_precipitacao_alertario`
)

{% endif %}

SELECT * FROM source_1

{% if not is_incremental() %}
UNION ALL
SELECT * FROM source_2
{% endif %}