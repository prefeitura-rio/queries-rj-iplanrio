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
    {{ clean_and_cast('num_folha', 'int64', trim=true) }} AS numero_folha,
    {{ clean_and_cast('numfunc', 'int64', trim=true) }} AS id_funcionario,
    {{ clean_and_cast('numvinc', 'int64', trim=true) }} AS id_vinculo,
    {{ clean_and_cast('numpens', 'int64', trim=true) }} AS id_pensionista,
    SAFE_CAST(TRIM(mes_ano_direito) AS date) AS mes_ano_direito,
    SAFE_CAST(TRIM(rubrica) AS int64) AS id_rubrica,
    SAFE_CAST(TRIM(tipo_rubrica) AS STRING) AS tipo_rubrica,
    SAFE_CAST(TRIM(desc_vant) AS STRING) AS desconto_vantagem,
    SAFE_CAST(TRIM(complemento) AS STRING) AS observacao,
    SAFE_CAST(REGEXP_REPLACE(valor, r',', '.') AS FLOAT64) AS valor,
    SAFE_CAST(REGEXP_REPLACE(correcao, r',', '.') AS FLOAT64) AS correcao,
    {{ clean_and_cast('emp_codigo', 'string', trim=true) }} AS id_empresa,
    SAFE_CAST(data_particao AS DATE) data_particao
FROM {{ source('brutos_ergon_staging', 'FICHAS_FINANCEIRAS') }} AS t