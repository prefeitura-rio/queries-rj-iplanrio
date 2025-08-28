{{
    config(
        alias='total_contagem',
    )
}}

SELECT
    SAFE_CAST(REGEXP_REPLACE(TRIM(numfunc), r'\.0$', '') AS STRING) AS id_funcionario,
    SAFE_CAST(REGEXP_REPLACE(TRIM(numvinc), r'\.0$', '') AS STRING) AS id_vinculo,
    SAFE_CAST(REGEXP_REPLACE(TRIM(chave), r'\.0$', '') AS STRING) AS id_chave,
    SAFE_CAST(TRIM(finalidade) AS STRING) AS finalidade,
    SAFE_CAST(TRIM(diastot) AS STRING) AS total_dias,
    SAFE_CAST(TRIM(diasfpub) AS STRING) AS diasfpub,
    SAFE_CAST(TRIM(diasfpubesp) AS STRING) AS diasfpubesp,
    SAFE_CAST(TRIM(total_periodos) AS STRING) AS total_periodos_contagem,
    SAFE_CAST(TRIM(total_anos) AS STRING) AS total_anos_contagem,
    SAFE_CAST(TRIM(data_proximo) AS STRING) AS data_previsao_proximo_periodo,
    SAFE_CAST(TRIM(nome_proximo) AS STRING) AS nome_finalidade,
    SAFE_CAST(REGEXP_REPLACE(TRIM(emp_codigo), r'\.0$', '') AS STRING) AS id_empresa,
FROM {{ source('brutos_ergon_staging', 'TOTAL_CONTA') }} AS t