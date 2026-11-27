{{
    config(
        materialized='table',
        alias='ficha_financeira_contabil',
        partition_by={
            "field": "updated_at",
            "data_type": "date",
            "granularity": "month",
        },
        tags=["raw", "ergon", "ficha_financeira_contabil", "salarios"],
        description="Tabela que contém os registros de valores de rubricas de fichas financeiras dos funcionários da Prefeitura da Cidade do Rio de Janeiro."
    )
}}

SELECT
    SAFE_CAST (mes_ano_folha AS DATE) AS mes_ano_folha,
    SAFE_CAST (numfunc AS string) AS id_funcionario,
    SAFE_CAST (numvinc AS string) AS id_vinculo,
    SAFE_CAST (numpens AS string) AS id_pensionista,
    SAFE_CAST (num_folha AS string) AS numero_folha,
    SAFE_CAST (
        REGEXP_REPLACE(TRIM(setor), r'\\.0$', '') AS string
    ) AS id_setor,
    SAFE_CAST (TRIM(secretaria) AS string) AS id_secretaria,
    SAFE_CAST (TRIM(tipo_func) AS STRING) AS tipo_funcionario,
    SAFE_CAST (detalha AS int64) AS detalhamento,
    SAFE_CAST (rubrica AS STRING) AS id_rubrica,
    SAFE_CAST (tipo_rubrica AS STRING) AS tipo_rubrica,
    SAFE_CAST (mes_ano_direito AS DATE) AS mes_ano_direito,
    SAFE_CAST (TRIM(desc_vant) AS int64) AS desconto_vantagem,
    SAFE_CAST (valor AS FLOAT64) AS valor,
    SAFE_CAST (TRIM(complemento) AS STRING) AS observacao,
    SAFE_CAST (TRIM(tipo_classif) AS STRING) AS tipo_classificacao,
    SAFE_CAST (TRIM(classificacao) AS int64) AS classificacao,
    SAFE_CAST (emp_codigo AS STRING) AS id_empresa,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon', 'ipl_pt_fichas') }} AS t

