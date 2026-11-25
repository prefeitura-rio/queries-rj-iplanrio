{{
    config(
        alias='metas_acordo_resultados_ordenacao'
    )
}}

SELECT
    SAFE_CAST(chave_meta_ar_2022 AS STRING) AS chave_meta_ar_2022,
    SAFE_CAST(ec AS STRING) AS ec,
    SAFE_CAST(descricao_meta_ar_2022_desdobrada AS STRING) AS descricao_meta_ar_2022_desdobrada,
    SAFE_CAST(numero_d AS STRING) AS numero_d,
    SAFE_CAST(codigo_egpweb AS STRING) AS codigo_egpweb,
    SAFE_CAST(relacionada_meta_estrategica AS STRING) AS relacionada_meta_estrategica,
    SAFE_CAST(orgao_ar AS STRING) AS orgao_ar
FROM {{ source('planejamento_gestao_dashboard_metas_staging', 'metas_acordo_resultados_ordenacao') }}
