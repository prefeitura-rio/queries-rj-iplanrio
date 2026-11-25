{{
    config(
        alias='acordo_resultado'
    )
}}

SELECT
    SAFE_CAST (ano AS INT64) AS ano,
    SAFE_CAST (orgao AS STRING) AS orgao,
    SAFE_CAST (orgao_egpweb AS STRING) AS orgao_egpweb,
    SAFE_CAST (tipo_acordo AS STRING) AS tipo_acordo,
    SAFE_CAST (data_publicacao AS DATE FORMAT 'DD/MM/YYYY') AS data_publicacao,
    SAFE_CAST (data_assinatura AS DATE) AS data_assinatura,
    SAFE_CAST (data_resultado AS DATE FORMAT 'DD/MM/YYYY') AS data_resultado,
    CASE
        WHEN bateu_acordo = "TRUE" THEN TRUE
        WHEN bateu_acordo = "FALSE" THEN FALSE
        WHEN bateu_acordo = "VERDADEIRO" THEN TRUE
        WHEN bateu_acordo = "FALSO" THEN FALSE
        ELSE NULL
    END AS indicador_cumprimento_acordo,
    SAFE_CAST (REPLACE(nota_final, ",", ".") AS FLOAT64) AS nota_final,
    SAFE_CAST (bonus_final AS FLOAT64) AS bonus_final,
    SAFE_CAST (valor_estimado_bonus AS FLOAT64) valor_estimado_bonus
FROM {{ source('planejamento_gestao_dashboard_metas_staging', 'acordo_resultado') }}
