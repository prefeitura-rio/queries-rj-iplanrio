{{
    config(
        materialized='table',
        alias='meteorologia_redemet',
        unique_key='primary_key',
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        post_hook='
        CREATE OR REPLACE TABLE `rj-iplanrio.clima_estacao_meteorologica_staging.meteorologia_redemet_last_partition_datario` AS (
            SELECT CURRENT_DATE("America/Sao_Paulo") AS data
        )'
    )
}}

SELECT
    SAFE_CAST(
        CONCAT(id_estacao, '_', data_medicao) AS STRING
    ) AS primary_key,
    SAFE_CAST(
        REGEXP_REPLACE(id_estacao, r'\.0$', '') AS STRING
        ) id_estacao,
    SAFE_CAST(
        SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', data_medicao) AS DATETIME
    ) AS data_medicao,
    SAFE_CAST(temperatura AS int64) temperatura,
    SAFE_CAST(umidade AS int64) umidade,
    SAFE_CAST(condicoes_tempo AS string) condicoes_tempo,
    SAFE_CAST(ceu AS string) ceu,
    SAFE_CAST(teto AS int64) teto,
    SAFE_CAST(visibilidade AS string) visibilidade,
    SAFE_CAST(ano AS int64) ano,
    SAFE_CAST(mes AS int64) mes,
    safe_cast(dia AS int64) dia,
    safe_cast(DATE(SAFE_CAST(ano as int64), SAFE_CAST(mes AS INT64), SAFE_CAST(dia AS INT64)) AS DATE) data,
FROM {{ source('clima_estacao_meteorologica_staging', 'meteorologia_redemet') }}

{% if is_incremental() %}

{% set max_partition = run_query(
    "
    SELECT DATE(gr)
    FROM (
        SELECT IF(
            MAX(data) > CURRENT_DATE('America/Sao_Paulo'),
            CURRENT_DATE('America/Sao_Paulo'),
            MAX(data)
        ) AS gr
        FROM `rj-iplanrio.clima_estacao_meteorologica_staging.meteorologia_redemet_last_partition_datario`
    )
    "
).columns[0].values()[0] %}

WHERE data >= DATE('{{ max_partition }}')

{% endif %}