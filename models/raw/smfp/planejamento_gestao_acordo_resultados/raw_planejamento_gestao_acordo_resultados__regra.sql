{{
    config(
        alias='regra'
    )
}}

SELECT
    SAFE_CAST(ano AS INT64) as ano,
    SAFE_CAST(orgaoid AS STRING) as id_orgao,
    SAFE_CAST(tipolinha AS STRING) as tipo_regra,
    SAFE_CAST(tipometaemun AS STRING) as tipo_meta,
    SAFE_CAST(SAFE_CAST(qtdmeta AS FLOAT64) AS INT64) as quantidade_meta,
    SAFE_CAST(nota AS FLOAT64) as nota,
    SAFE_CAST(bonus AS FLOAT64) as bonus
FROM {{ source('planejamento_gestao_dashboard_metas_staging', 'regra') }}
