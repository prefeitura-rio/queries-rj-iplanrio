{{
    config(
        alias='avaliacao_meta'
    )
}}

SELECT
    SAFE_CAST (ano AS INT64) AS ano,
    SAFE_CAST (cod_chance AS INT64) AS codigo_chance,
    SAFE_CAST (cor_chance AS STRING) AS cor_chance,
    SAFE_CAST (descricao_ar AS STRING) AS legenda_chance_ar,
    SAFE_CAST (descricao_unificada_pe_ar AS STRING) AS legenda_chance_unificada,
    SAFE_CAST (caso AS INT64) AS caso
FROM {{ source('planejamento_gestao_dashboard_metas_staging', 'avaliacao_meta') }}
