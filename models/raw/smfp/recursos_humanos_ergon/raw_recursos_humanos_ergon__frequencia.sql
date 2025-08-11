
{{
    config(
        materialized='incremental',
        alias='raw_recursos_humanos_ergon__frequencia',
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        }
    )
}}

SELECT
    SAFE_CAST(REGEXP_REPLACE(TRIM(numfunc), r'\.0$', '') AS STRING) AS id_funcionario,
    SAFE_CAST(REGEXP_REPLACE(TRIM(numvinc), r'\.0$', '') AS STRING) AS id_vinculo,
    SAFE_CAST(TRIM(dtini) AS STRING) AS data_inicio,
    SAFE_CAST(TRIM(dtfim) AS STRING) AS data_final,
    SAFE_CAST(TRIM(tipofreq) AS STRING) AS tipo_frequencia,
    SAFE_CAST(REGEXP_REPLACE(TRIM(codfreq), r'\.0$', '') AS STRING) AS id_frequencia,
    SAFE_CAST(TRIM(obs) AS STRING) AS observacoes,
    SAFE_CAST(REGEXP_REPLACE(TRIM(emp_codigo), r'\.0$', '') AS STRING) AS id_empresa,
    SAFE_CAST(data_particao AS DATE) data_particao,
FROM {{ source('recursos_humanos_ergon', 'frequencia') }} AS t
WHERE
    SAFE_CAST(data_particao AS DATE) < CURRENT_DATE('America/Sao_Paulo')

{% if is_incremental() %}

{% set max_partition = run_query("SELECT gr FROM (SELECT IF(max(data_particao) > CURRENT_DATE('America/Sao_Paulo'), CURRENT_DATE('America/Sao_Paulo'), max(data_particao)) as gr FROM " ~ this ~ ")").columns[0].values()[0] %}

AND
    SAFE_CAST(data_particao AS DATE) > ("{{ max_partition }}")

{% endif %}