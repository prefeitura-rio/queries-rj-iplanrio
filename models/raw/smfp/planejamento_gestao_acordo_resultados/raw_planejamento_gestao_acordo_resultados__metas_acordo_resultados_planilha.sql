{{
    config(
        alias='metas_acordo_resultados_planilha'
    )
}}

SELECT
    SAFE_CAST(chave_meta_ar_2022 AS STRING) AS chave_meta_ar_2022,
    SAFE_CAST(orgao_ar AS STRING) AS orgao_ar,
    SAFE_CAST(orgao_resp AS STRING) AS orgao_resp,
    SAFE_CAST(numero AS STRING) AS numero,
    SAFE_CAST(numero_d AS STRING) AS numero_d,
    SAFE_CAST(ec AS STRING) AS ec,
    SAFE_CAST(descricao_meta_ar_2022 AS STRING) AS descricao_meta_ar_2022,
    SAFE_CAST(descricao_meta_ar_2022_desdobrada AS STRING) AS descricao_meta_ar_2022_desdobrada,
    SAFE_CAST(fonte AS STRING) AS fonte,
    SAFE_CAST(valor_referencia AS STRING) AS valor_referencia,
    SAFE_CAST(data_referencia AS STRING) AS data_referencia,
    SAFE_CAST(meta_2022 AS STRING) AS meta_2022,
    SAFE_CAST(codigo_egpweb AS STRING) AS codigo_egpweb,
    SAFE_CAST(relacionada_meta_estrategica AS STRING) AS relacionada_meta_estrategica,
    SAFE_CAST(unidade_medida AS STRING) AS unidade_medida,
    SAFE_CAST(gerencia AS STRING) AS gerencia,
    SAFE_CAST(codigo_meta_ar AS STRING) AS codigo_meta_ar,
    SAFE_CAST(SAFE_CAST(ano AS FLOAT64) AS INT64) AS ano,
    SAFE_CAST(SAFE_CAST(ref_resultado AS FLOAT64) AS INT64) AS ref_resultado,
    SAFE_CAST(resultado AS STRING) AS resultado,
    SAFE_CAST(SAFE_CAST(ref_chance AS FLOAT64) AS INT64) AS ref_chance,
    SAFE_CAST(SAFE_CAST(chance AS FLOAT64) AS INT64) AS chance,
    SAFE_CAST(SAFE_CAST(ref_resultado_2 AS FLOAT64) AS INT64) AS ref_resultado_2,
    SAFE_CAST(resultado_2 AS STRING) AS resultado_2,
    SAFE_CAST(SAFE_CAST(ref_chance_2 AS FLOAT64) AS INT64) AS ref_chance_2,
    SAFE_CAST(SAFE_CAST(chance_2 AS FLOAT64) AS INT64) AS chance_2,
    SAFE_CAST(resultado_orgao_1 AS STRING) AS resultado_orgao_1,
    SAFE_CAST(resultado_orgao_2_performance AS STRING) AS resultado_orgao_2_performance,
    SAFE_CAST(SAFE_CAST(caso_chance AS FLOAT64) AS INT64) AS caso_chance,
    SAFE_CAST(SAFE_CAST(caso_chance_orgao_2_performance AS FLOAT64) AS INT64) AS caso_chance_orgao_2_performance,
    SAFE_CAST(SAFE_CAST(pior_caso_chance AS FLOAT64) AS INT64) AS pior_caso_chance,
    SAFE_CAST(SAFE_CAST(cor_chance_meta AS FLOAT64) AS INT64) AS cor_chance_meta,
    SAFE_CAST(SAFE_CAST(cor_chance_meta_orgao_2 AS FLOAT64) AS INT64) AS cor_chance_meta_orgao_2
FROM {{ source('planejamento_gestao_dashboard_metas_staging', 'metas_acordo_resultados_planilha') }}
