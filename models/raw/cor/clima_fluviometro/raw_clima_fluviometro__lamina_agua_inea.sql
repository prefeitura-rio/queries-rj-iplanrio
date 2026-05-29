{{
    config(
        alias="lamina_agua_inea",
        materialized='table',
        unique_key="primary_key",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "month", 
        },
        post_hook='CREATE OR REPLACE TABLE `rj-iplanrio.clima_fluviometro_staging.lamina_agua_inea_last_partition` AS (SELECT CURRENT_DATETIME("America/Sao_Paulo") AS data)'
    )
}}

SELECT
    DISTINCT
    CONCAT(id_estacao, '_', data) AS primary_key,
    SAFE_CAST(id_estacao AS STRING) AS id_estacao,
    SAFE_CAST(
            SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', data) AS DATETIME
        ) AS data_medicao,
    SAFE_CAST(altura_agua AS FLOAT64) altura_agua,
    SAFE_CAST(DATE_TRUNC(DATE(data), day) AS DATE) data,
FROM {{ source('clima_fluviometro_staging', 'lamina_agua_inea') }}


{% if is_incremental() %}

    {% set max_partition = run_query(
        "SELECT DATE(gr) FROM (
            SELECT IF(
                max(data) > CURRENT_DATE('America/Sao_Paulo'), 
                CURRENT_DATE('America/Sao_Paulo'), 
                max(data)
            ) as gr 
            FROM `rj-iplanrio.clima_fluviometro_staging.lamina_agua_inea_last_partition`
        )").columns[0].values()[0] %}
    WHERE
        ano_particao >= SAFE_CAST(EXTRACT(YEAR FROM DATE(("{{ max_partition }}"))) AS STRING) AND
        mes_particao >= SAFE_CAST(EXTRACT(MONTH FROM DATE(("{{ max_partition }}"))) AS STRING) AND
        data >= SAFE_CAST(DATE_TRUNC(DATE(("{{ max_partition }}")), day) AS STRING)

    {% endif %} 