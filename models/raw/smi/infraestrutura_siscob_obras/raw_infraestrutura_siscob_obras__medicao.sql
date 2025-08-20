{{
    config(
        alias="medicao",
        schema='infraestrutura_siscob_obras',
        materialized="table",
    )
}}

SELECT 
    DISTINCT
        SAFE_CAST(REGEXP_REPLACE(cd_obra, r'\.0$', '') AS STRING) id_obra,
        SAFE_CAST(
            REGEXP_REPLACE(nr_medicao, r'\.0$', '') AS STRING
        ) id_medicao,
        SAFE_CAST(REGEXP_REPLACE(cd_etapa, r'\.0$', '') AS STRING) id_etapa,
        SAFE_CAST(tp_medicao_d AS STRING) tipo_medicao,
        SAFE_CAST(
            SAFE.PARSE_DATE ('%Y-%m-%d', dt_ini_medicao) AS DATE
        ) AS data_inicio,
        SAFE_CAST(
            SAFE.PARSE_DATE ('%Y-%m-%d', dt_fim_medicao) AS DATE
        ) AS data_fim,
        SAFE_CAST(vl_final AS FLOAT64) valor_final,
FROM {{ source('brutos_siscob_staging', 'medicao') }} AS t