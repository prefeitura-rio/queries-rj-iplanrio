{{
    config(
        alias='orgaos'
    )
}}

SELECT
    SAFE_CAST(sigla AS STRING) AS sigla,
    SAFE_CAST(orgao AS STRING) AS orgao,
    SAFE_CAST(SAFE_CAST(ordem_secretariado_relatorios AS FLOAT64) AS INT64) AS ordem_secretariado_relatorios,
    SAFE_CAST(SAFE_CAST(ordem_ar2021 AS FLOAT64) AS INT64) AS ordem_ar2021,
    SAFE_CAST(SAFE_CAST(ordem_ar2022 AS FLOAT64) AS INT64) AS ordem_ar2022,
    SAFE_CAST(status AS STRING) AS status,
    SAFE_CAST(coluna_antiga_ordem AS STRING) AS coluna_antiga_ordem,
    SAFE_CAST(coluna_antiga_ordem_2 AS STRING) AS coluna_antiga_ordem_2,
    SAFE_CAST(coluna_antiga_ordem_3 AS STRING) AS coluna_antiga_ordem_3,
    SAFE_CAST(balanco AS STRING) AS balanco,
    SAFE_CAST(orgao_ar AS STRING) AS orgao_ar
FROM {{ source('planejamento_gestao_dashboard_metas_staging', 'orgaos') }}
