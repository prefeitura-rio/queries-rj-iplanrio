{{
    config(
        alias="taxa_precipitacao_alertario",
        materialized='incremental',
        unique_key='primary_key',
        partition_by={
            "field": "data_particao",
            "data_type": "date",
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
-- Obtendo os dados de 1997 até 2024 de acordo com a tabela taxa_precipitacao_alertario no bucket de rj-cor
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
),
-- Como há um furo nos dados de 2025-01-01 até 2026-06-01 no bucket de rj-cor taxa_precipitacao_alertario, 
-- Vamos completar os dados de acordo com a tabela taxa_precipitacao_alertario_5min no bucket de rj-cor que possui uma granularidade menor.
source_3 as (
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
        SAFE_CAST(data_medicao as DATE) as data_particao
        FROM `rj-cor.clima_pluviometro_staging.taxa_precipitacao_alertario_5min`
    where DATE(data_medicao) between '2025-01-01' and '2026-06-01'
)

{% endif %}

SELECT * FROM source_1

{% if not is_incremental() %}
UNION ALL
SELECT * FROM source_2
UNION ALL
SELECT * FROM source_3
{% endif %}