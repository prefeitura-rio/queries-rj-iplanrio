{{
    config(
        alias="meteorologia_alertario",
        materialized='table',
        unique_key="primary_key",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

SELECT
    DISTINCT
    SAFE_CAST(
        REGEXP_REPLACE(id_estacao, r'\.0$', '') AS STRING
        ) id_estacao,
    SAFE_CAST(
        SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', data_medicao) AS DATETIME
    ) AS data_medicao,
    SAFE_CAST(temperatura AS FLOAT64) temperatura,
    SAFE_CAST(pressao_atmosferica AS FLOAT64) pressao,
    SAFE_CAST(temperatura_orvalho AS FLOAT64) temperatura_orvalho,
    SAFE_CAST(SAFE_CAST(umidade_ar AS FLOAT64) AS INT64) umidade,
    SAFE_CAST(SAFE_CAST(direcao_vento AS FLOAT64) AS INT64) direcao_vento,
    SAFE_CAST(velocidade_vento AS FLOAT64) velocidade_vento,
    SAFE_CAST(sensacao_termica AS FLOAT64) sensacao_termica,
    SAFE_CAST(DATE_TRUNC(DATE(data_medicao), day) AS DATE) data,
    CONCAT(id_estacao, '_', data_medicao) AS primary_key,

FROM {{ source('clima_estacao_meteorologica_staging', 'meteorologia_alertario') }}

{% if is_incremental() %}

where DATETIME(data_medicao) > (SELECT MAX(DATETIME(data_medicao)) FROM {{ this }})

{% endif %}