{{
    config( alias='ficha_financeira',schema='recursos_humanos_ergon_comlurb')
}}

SELECT
    SAFE_CAST(REGEXP_REPLACE(TRIM(numfunc), r'\.0$', '') AS STRING) AS id_funcionario,
    SAFE_CAST(REGEXP_REPLACE(TRIM(numvinc), r'\.0$', '') AS STRING) AS id_vinculo,
    SAFE_CAST(REGEXP_REPLACE(TRIM(numpens), r'\.0$', '') AS INT64) AS numero_pensionista,
    SAFE_CAST(TRIM(mes_ano_direito) AS STRING) AS data_pagamento,
    SAFE_CAST(TRIM(rubrica) AS STRING) AS rubrica,
    SAFE_CAST(TRIM(tipo_rubrica) AS STRING) AS tipo_rubrica,
    SAFE_CAST(TRIM(desc_vant) AS STRING) AS desconto,
    SAFE_CAST(TRIM(complemento) AS STRING) AS complemento_rubrica,
    SAFE_CAST(REGEXP_REPLACE(valor, r',', '.') AS FLOAT64) AS valor_rubrica,    SAFE_CAST(REGEXP_REPLACE(correcao, r',', '.') AS FLOAT64) AS correcao,    SAFE_CAST(REGEXP_REPLACE(TRIM(emp_codigo), r'\.0$', '') AS STRING) AS id_empresa,
    SAFE_CAST(DATE(mes_ano_folha) AS DATE) AS data_folha,
    SAFE_CAST(REGEXP_REPLACE(TRIM(num_folha), r'\.0$', '') AS INT64) AS numero_folha,
FROM rj-smfp.recursos_humanos_ergon_comlurb_staging.ficha_financeira AS t