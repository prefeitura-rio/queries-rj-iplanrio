{{
    config(
        alias='raw_recursos_humanos_ergon__pre_contagem',
    )
}}

SELECT
    SAFE_CAST(TRIM(finalidade) AS STRING) AS finalidade,
    SAFE_CAST(REGEXP_REPLACE(TRIM(numfunc), r'\.0$', '') AS STRING) AS id_funcionario,
    SAFE_CAST(REGEXP_REPLACE(TRIM(numvinc), r'\.0$', '') AS STRING) AS id_vinculo,
    SAFE_CAST(TRIM(periodos) AS STRING) AS periodos,
    SAFE_CAST(TRIM(offset) AS STRING) AS dias,
    SAFE_CAST(TRIM(dtini) AS STRING) AS data_validade,
    SAFE_CAST(REGEXP_REPLACE(TRIM(emp_codigo), r'\.0$', '') AS STRING) AS id_empresa,
    SAFE_CAST(TRIM(flex_campo_01) AS STRING) AS observacoes,
FROM {{ source('recursos_humanos_ergon', 'pre_contagem') }} AS t