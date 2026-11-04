{{
    config(
        alias="medicao",
        schema='infraestrutura_siscob_obras',
        materialized="table",
    )
}}

SELECT DISTINCT
    SAFE_CAST (cd_obra AS STRING) id_obra,
    SAFE_CAST (nr_medicao AS STRING) id_medicao,
    SAFE_CAST (cd_etapa AS STRING) id_etapa,
    SAFE_CAST (tp_medicao_d AS STRING) tipo_medicao,
    SAFE_CAST (dt_ini_medicao AS DATE) AS data_inicio,
    SAFE_CAST (dt_fim_medicao AS DATE) AS data_fim,
    SAFE_CAST (vl_final AS FLOAT64) valor_final,
FROM {{ source('brutos_siscob_staging', 'medicao') }} AS t