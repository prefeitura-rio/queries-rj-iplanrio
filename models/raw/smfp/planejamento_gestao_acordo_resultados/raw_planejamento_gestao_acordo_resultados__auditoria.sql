{{
    config(
        alias='auditoria'
    )
}}

SELECT
    SAFE_CAST (cod_auditoria AS STRING) AS id_auditoria,
    SAFE_CAST (ano_base AS INT64) AS ano,
    SAFE_CAST (orgao_auditado AS STRING) AS orgao,
    SAFE_CAST (cod_meta AS STRING) AS codigo_meta,
    SAFE_CAST (orgao_acordo_egpweb AS STRING) AS orgao_egpweb,
    SAFE_CAST (relatorio_auditoria_geral_rag AS STRING) AS rag,
    SAFE_CAST (ordem_servico AS STRING) AS ordem_servico,
    SAFE_CAST (
        SAFE_CAST (data_inicio_auditoria AS STRING) AS DATE FORMAT 'DD/MM/YYYY'
    ) AS data_inicio_auditoria,
    SAFE_CAST (
        SAFE_CAST (data_final_auditoria AS STRING) AS DATE FORMAT 'DD/MM/YYYY'
    ) AS data_final_auditoria,
    SAFE_CAST (
        SAFE_CAST (data_relatorio AS STRING) AS DATE FORMAT 'DD/MM/YYYY'
    ) AS data_relatorio,
    SAFE_CAST (meta_auditada AS STRING) AS descricao_meta,
    SAFE_CAST (resumo_auditoria AS STRING) AS resumo_auditoria,
    SAFE_CAST (numero_testes AS INT64) AS quantidade_testes,
    SAFE_CAST (numero_fragilidades AS INT64) AS quantidade_fragilidades,
    CASE
        WHEN penalidade = "TRUE" THEN TRUE
        WHEN penalidade = "FALSE" THEN FALSE
        WHEN penalidade = "VERDADEIRO" THEN TRUE
        WHEN penalidade = "FALSO" THEN FALSE
        ELSE NULL
    END AS indicador_penalidade,
    SAFE_CAST (
        REPLACE(SAFE_CAST (penalidade_nota AS STRING), ',', '.') AS FLOAT64
    ) AS penalidade_nota,
    SAFE_CAST (
        REPLACE(SAFE_CAST (penalidade_bonus AS STRING), ',', '.') AS FLOAT64
    ) AS penalidade_bonus,
    SAFE_CAST (
        SAFE_CAST (inicio_periodo_apurado AS STRING) AS DATE FORMAT 'DD/MM/YYYY'
    ) AS inicio_periodo_apurado,
    SAFE_CAST (
        SAFE_CAST (final_periodo_apurado AS STRING) AS DATE FORMAT 'DD/MM/YYYY'
    ) AS final_periodo_apurado,
    SAFE_CAST (numero_recomendacoes AS INT64) AS quantidade_recomendacoes
FROM {{ source('planejamento_gestao_dashboard_metas_staging', 'auditoria') }}
