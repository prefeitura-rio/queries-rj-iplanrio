{{
    config(
        materialized='view',
        alias='ficha_financeira_contabil',
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
        tags=["raw", "ergon", "ficha_financeira_contabil", "salarios"],
        description="Tabela que contém os registros de valores de rubricas de fichas financeiras dos funcionários da Prefeitura da Cidade do Rio de Janeiro."
    )
}}

SELECT
    SAFE_CAST(mes_ano_folha AS DATE) AS mes_ano_folha,
    SAFE_CAST(REGEXP_REPLACE(TRIM(numfunc), r'\\.0$', '') AS int64) AS id_funcionario,
    SAFE_CAST(REGEXP_REPLACE(TRIM(numvinc), r'\\.0$', '') AS int64) AS id_vinculo,
    SAFE_CAST(REGEXP_REPLACE(TRIM(numpens), r'\\.0$', '') AS INT64) AS id_pensionista,
    SAFE_CAST(TRIM(num_folha) AS INT64) AS numero_folha,
    SAFE_CAST(REGEXP_REPLACE(TRIM(setor), r'\\.0$', '') AS int64) AS id_setor,
    SAFE_CAST(TRIM(secretaria) AS int64) AS id_secretaria,
    SAFE_CAST(TRIM(tipo_func) AS STRING) AS tipo_funcionario,
    SAFE_CAST(TRIM(detalha) AS int64) AS detalhamento,
    SAFE_CAST(TRIM(rubrica) AS STRING) AS id_rubrica,
    SAFE_CAST(TRIM(tipo_rubrica) AS STRING) AS tipo_rubrica,
    SAFE_CAST(mes_ano_direito AS DATE) AS mes_ano_direito,
    SAFE_CAST(TRIM(desc_vant) AS int64) AS desconto_vantagem,
    SAFE_CAST(REGEXP_REPLACE(valor, r',', '.') AS FLOAT64) AS valor,
    SAFE_CAST(TRIM(complemento) AS STRING) AS observacao,
    SAFE_CAST(TRIM(tipo_classif) AS STRING) AS tipo_classificacao,
    SAFE_CAST(TRIM(classificacao) AS int64) AS classificacao,
    SAFE_CAST(REGEXP_REPLACE(TRIM(emp_codigo), r'\\.0$', '') AS STRING) AS id_empresa,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM {{ source('brutos_ergon_staging', 'IPL_PT_FICHAS') }} AS t

