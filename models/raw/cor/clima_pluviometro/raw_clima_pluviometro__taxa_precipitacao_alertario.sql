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
    * EXCEPT(data),
    PARSE_DATE('%Y-%m-%d', data) AS data
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