{{
    config(
        materialized='table',
        alias='ficha_financeira',
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        }
    )
}}

SELECT
    SAFE_CAST(DATE(mes_ano_folha) AS DATE) AS mes_ano_folha,
    SAFE_CAST(REGEXP_REPLACE(TRIM(num_folha), r'\.0$', '') AS INT64) AS num_folha,
    SAFE_CAST(REGEXP_REPLACE(TRIM(numfunc), r'\.0$', '') AS STRING) AS numfunc,
    SAFE_CAST(REGEXP_REPLACE(TRIM(numvinc), r'\.0$', '') AS STRING) AS numvinc,
    SAFE_CAST(REGEXP_REPLACE(TRIM(numpens), r'\.0$', '') AS INT64) AS numpens,
    SAFE_CAST(TRIM(mes_ano_direito) AS STRING) AS mes_ano_direito,
    SAFE_CAST(TRIM(rubrica) AS STRING) AS rubrica,
    SAFE_CAST(TRIM(tipo_rubrica) AS STRING) AS tipo_rubrica,
    SAFE_CAST(TRIM(desc_vant) AS STRING) AS desc_vant,
    SAFE_CAST(TRIM(complemento) AS STRING) AS complemento,
    SAFE_CAST(REGEXP_REPLACE(valor, r',', '.') AS FLOAT64) AS valor,
    SAFE_CAST(REGEXP_REPLACE(correcao, r',', '.') AS FLOAT64) AS correcao,
    SAFE_CAST(REGEXP_REPLACE(TRIM(emp_codigo), r'\.0$', '') AS STRING) AS emp_codigo,
    SAFE_CAST(data_particao AS DATE) data_particao,
FROM {{ source('recursos_humanos_ergon', 'ficha_financeira') }} AS t
-- WHERE
--     SAFE_CAST(data_particao AS DATE) < CURRENT_DATE('America/Sao_Paulo')

-- {% if is_incremental() %}

-- {% set max_partition = run_query("SELECT gr FROM (SELECT IF(max(data_particao) > CURRENT_DATE('America/Sao_Paulo'), CURRENT_DATE('America/Sao_Paulo'), max(data_particao)) as gr FROM " ~ this ~ ")").columns[0].values()[0] %}

-- AND
--     SAFE_CAST(data_particao AS DATE) > ("{{ max_partition }}")

-- {% endif %}