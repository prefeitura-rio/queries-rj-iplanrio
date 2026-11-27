{{
    config(
        materialized='table',
        alias='ficha_financeira',
        partition_by={
            "field": "updated_at",
            "data_type": "date",
            "granularity": "month",
        },
        tags=["raw", "ergon", "ficha_financeira", "salarios"],
        description="Tabela que contém os registros de valores de rubricas de fichas financeiras dos funcionários da Prefeitura da Cidade do Rio de Janeiro."
    )
}}

SELECT
    SAFE_CAST (DATE(mes_ano_folha) AS DATE) AS mes_ano_folha,
    SAFE_CAST (num_folha AS string) AS numero_folha,
    SAFE_CAST (numfunc AS string) AS id_funcionario,
    SAFE_CAST (numvinc AS string) AS id_vinculo,
    SAFE_CAST (numpens AS string) AS id_pensionista,
    SAFE_CAST (mes_ano_direito AS date) AS mes_ano_direito,
    SAFE_CAST (rubrica AS string) AS id_rubrica,
    SAFE_CAST (tipo_rubrica AS STRING) AS tipo_rubrica,
    SAFE_CAST (desc_vant AS STRING) AS desconto_vantagem,
    SAFE_CAST (complemento AS STRING) AS observacao,
    SAFE_CAST (valor AS FLOAT64) AS valor,
    SAFE_CAST (correcao AS FLOAT64) AS correcao,
    SAFE_CAST (emp_codigo AS STRING) AS id_empresa,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon', 'fichas_financeiras') }} AS t