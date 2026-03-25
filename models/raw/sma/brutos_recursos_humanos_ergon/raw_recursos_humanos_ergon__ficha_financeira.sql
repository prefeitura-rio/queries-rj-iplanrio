{{
    config(
        materialized='table',
        alias='ficha_financeira',
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
        tags=["raw", "ergon", "ficha_financeira", "salarios"],
        description="Tabela que contém os registros de valores de rubricas de fichas financeiras dos funcionários da Prefeitura da Cidade do Rio de Janeiro."
    )
}}

SELECT
    SAFE_CAST(DATE(mes_ano_folha) AS DATE) AS mes_ano_folha,
    SAFE_CAST(REGEXP_REPLACE(TRIM(num_folha), r'\.0$', '') AS string) AS numero_folha,
    SAFE_CAST(REGEXP_REPLACE(TRIM(numfunc), r'\.0$', '') AS string) AS id_funcionario,
    SAFE_CAST(REGEXP_REPLACE(TRIM(numvinc), r'\.0$', '') AS string) AS id_vinculo,
    SAFE_CAST(REGEXP_REPLACE(TRIM(numpens), r'\.0$', '') AS string) AS id_pensionista,
    SAFE_CAST(TRIM(mes_ano_direito) AS date) AS mes_ano_direito,
    SAFE_CAST(TRIM(rubrica) AS string) AS id_rubrica,
    SAFE_CAST(TRIM(tipo_rubrica) AS STRING) AS tipo_rubrica,
    SAFE_CAST(TRIM(desc_vant) AS STRING) AS desconto_vantagem,
    SAFE_CAST(TRIM(complemento) AS STRING) AS observacao,
    SAFE_CAST(REGEXP_REPLACE(valor, r',', '.') AS FLOAT64) AS valor,
    SAFE_CAST(REGEXP_REPLACE(correcao, r',', '.') AS FLOAT64) AS correcao,
    SAFE_CAST(REGEXP_REPLACE(TRIM(emp_codigo), r'\.0$', '') AS STRING) AS id_empresa,
    SAFE_CAST(data_particao AS DATE) data_particao
FROM {{ source('brutos_ergon_staging', 'FICHAS_FINANCEIRAS') }} AS t