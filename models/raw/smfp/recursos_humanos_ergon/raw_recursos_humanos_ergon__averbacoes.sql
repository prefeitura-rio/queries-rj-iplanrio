{{
    config(
        alias='raw_recursos_humanos_ergon__averbacoes',
    )
}}

SELECT
    SAFE_CAST(REGEXP_REPLACE(TRIM(numfunc), r'\.0$', '') AS STRING) AS id_funcionario,
    SAFE_CAST(REGEXP_REPLACE(TRIM(numvinc), r'\.0$', '') AS STRING) AS id_vinculo,
    SAFE_CAST(REGEXP_REPLACE(TRIM(chave), r'\.0$', '') AS STRING) AS id_averbacao,
    SAFE_CAST(TRIM(dtini) AS STRING) AS data_inicio,
    SAFE_CAST(TRIM(dtfim) AS STRING) AS data_final,
    SAFE_CAST(TRIM(instituicao) AS STRING) AS instituicao,
    SAFE_CAST(REGEXP_REPLACE(TRIM(tipotempo), r'\.0$', '') AS STRING) AS id_tipo_tempo,
    SAFE_CAST(TRIM(data_a_contar) AS STRING) AS data_validade,
    SAFE_CAST(TRIM(total_dias) AS STRING) AS total_dias_averbados,
    SAFE_CAST(TRIM(motivo) AS STRING) AS motivo,
    SAFE_CAST(TRIM(sobrepoe) AS STRING) AS sobrepoe,
    SAFE_CAST(REGEXP_REPLACE(TRIM(emp_codigo), r'\.0$', '') AS STRING) AS id_empresa,
    SAFE_CAST(TRIM(obs) AS STRING) AS obs,
    SAFE_CAST(TRIM(regprev) AS STRING) AS regime_previdenciario,
FROM {{ source('recursos_humanos_ergon', 'averbacoes') }} AS t