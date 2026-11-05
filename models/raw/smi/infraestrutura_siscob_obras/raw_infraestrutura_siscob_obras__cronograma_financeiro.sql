{{
    config(
        alias="cronograma_financeiro",
        schema='infraestrutura_siscob_obras',
        materialized="table",
    )
}}

SELECT 
    DISTINCT
        SAFE_CAST(cd_obra AS STRING) id_obra,
        SAFE_CAST(etapa AS STRING) id_etapa,
        SAFE_CAST(
            dt_inicio_etapa AS DATE
        ) AS data_inicio,
        SAFE_CAST(
            dt_fim_etapa AS DATE
        ) AS data_fim,
        SAFE_CAST(vl_estimado AS FLOAT64) valor_estimado,
        SAFE_CAST(pc_percentual AS FLOAT64) percentual_estimado
FROM {{ source('brutos_siscob_staging', 'cronograma_financeiro') }} AS t