{{
    config(
        alias='relacao_metas'
    )
}}

SELECT
    SAFE_CAST(chave_meta_pe AS STRING) AS chave_meta_pe,
    SAFE_CAST(texto_meta_pe AS STRING) AS texto_meta_pe,
    SAFE_CAST(chave_meta_ar AS STRING) AS chave_meta_ar,
    SAFE_CAST(texto_meta_ar AS STRING) AS texto_meta_ar,
    SAFE_CAST(relacao_entre_indicadores AS STRING) AS relacao_entre_indicadores,
    SAFE_CAST(meta_pe AS STRING) AS meta_pe,
    SAFE_CAST(ar_2022 AS STRING) AS ar_2022,
    SAFE_CAST(chave_meta_ar_egpweb AS STRING) AS chave_meta_ar_egpweb,
    SAFE_CAST(SAFE_CAST(ordem_meta_no_orgao AS FLOAT64) AS INT64) AS ordem_meta_no_orgao,
    SAFE_CAST(orgao_pe AS STRING) AS orgao_pe,
    SAFE_CAST(orgao_ar AS STRING) AS orgao_ar
FROM {{ source('planejamento_gestao_dashboard_metas_staging', 'relacao_metas') }}
