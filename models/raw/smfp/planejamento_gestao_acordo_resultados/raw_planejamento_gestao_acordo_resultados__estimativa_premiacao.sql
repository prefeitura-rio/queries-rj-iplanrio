{{
    config(
        alias='estimativa_premiacao'
    )
}}

SELECT
    SAFE_CAST (ano AS INT64) AS ano,
    SAFE_CAST (orgao AS STRING) AS orgao,
    SAFE_CAST (REPLACE(estimativa, ',', '.') AS FLOAT64) AS estimativa_premiacao,
    SAFE_CAST (ano_folha AS INT64) AS ano_referencia,
    SAFE_CAST (data_estimativa AS DATE FORMAT 'DD/MM/YYYY') AS data_estimativa
FROM {{ source('planejamento_gestao_dashboard_metas_staging', 'estimativa_premiacao') }}
