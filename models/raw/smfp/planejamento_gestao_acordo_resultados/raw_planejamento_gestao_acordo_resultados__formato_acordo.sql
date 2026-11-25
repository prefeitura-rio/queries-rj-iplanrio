{{
    config(
        alias='formato_acordo'
    )
}}

SELECT
    SAFE_CAST (ano AS INT64) AS ano,
    SAFE_CAST (t.formato_acordo AS STRING) AS formato,
    CASE
        WHEN performance = "TRUE" THEN TRUE
        WHEN performance = "FALSE" THEN FALSE
        WHEN performance = "VERDADEIRO" THEN TRUE
        WHEN performance = "FALSO" THEN FALSE
        ELSE NULL
    END AS indicador_performance,
    SAFE_CAST (SAFE_CAST (ano_pagamento AS FLOAT64) AS INT64) AS ano_pagamento,
    SAFE_CAST (justificativa_falta_pagamento AS STRING) AS justificativa_pagamento,
    SAFE_CAST (
        REPLACE(nota_minima_bonificacao, ",", ".") AS FLOAT64
    ) AS nota_minima_bonificacao,
    SAFE_CAST (
        REPLACE(bonificacao_minima_sem_auditoria, ",", ".") AS FLOAT64
    ) AS bonificacao_minima,
    SAFE_CAST (
        REPLACE(bonificacao_maxima_sem_auditoria, ",", ".") AS FLOAT64
    ) AS bonificacao_maxima
FROM {{ source('planejamento_gestao_dashboard_metas_staging', 'formato_acordo') }} AS t
