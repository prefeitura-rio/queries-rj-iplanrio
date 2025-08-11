
{{
    config(
        materialized='incremental',
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
        alias='vantagens',
    )
}}

SELECT
    SAFE_CAST(REGEXP_REPLACE(TRIM(numfunc), r'\.0$', '') AS STRING) AS id_funcionario,
    SAFE_CAST(REGEXP_REPLACE(TRIM(numvinc), r'\.0$', '') AS STRING) AS id_vinculo,
    SAFE_CAST(TRIM(vantagem) AS STRING) AS nome_vantagem,
    SAFE_CAST(TRIM(SUBSTR(dtini, 1, 10)) AS DATE) AS data_inicio,
    SAFE_CAST(TRIM(SUBSTR(dtfim,1 , 10)) AS DATE) AS data_final,
    SAFE_CAST(TRIM(valor) AS STRING) AS valor_vantagem,
    SAFE_CAST(TRIM(info) AS STRING) AS informacao_atributo,
    SAFE_CAST(TRIM(tipo_incorporacao) AS STRING) AS tipo_incorporacao_cargo_fiducia,
    SAFE_CAST(TRIM(perc_inc_funcao) AS STRING) AS percentual_incorporacao_cargo_fiducia,
    SAFE_CAST(TRIM(inc_tabelavenc) AS STRING) AS incide_tabela_vencimentos,
    SAFE_CAST(TRIM(inc_referencia) AS STRING) AS incide_tabela_simbolo,
    SAFE_CAST(TRIM(obs) AS STRING) AS observacoes,
    SAFE_CAST(TRIM(valor2) AS STRING) AS valor_complementar,
    SAFE_CAST(TRIM(info2) AS STRING) AS informacao_complementar,
    SAFE_CAST(TRIM(valor3) AS STRING) AS valor_complementar_2,
    SAFE_CAST(TRIM(info3) AS STRING) AS informacao_complementar_2,
    SAFE_CAST(TRIM(valor4) AS STRING) AS valor_coplementar_3,
    SAFE_CAST(TRIM(info4) AS STRING) AS informacao_complementar_3,
    SAFE_CAST(TRIM(valor5) AS STRING) AS valor_complementar_4,
    SAFE_CAST(TRIM(info5) AS STRING) AS informacao_complementar_4,
    SAFE_CAST(TRIM(valor6) AS STRING) AS valor_complementar_5,
    SAFE_CAST(TRIM(info6) AS STRING) AS informacao_complementar_5,
    SAFE_CAST(TRIM(flex_campo_05) AS STRING) AS valor_incorporado,
    SAFE_CAST(REGEXP_REPLACE(TRIM(emp_codigo), r'\.0$', '') AS STRING) AS id_empresa,
    SAFE_CAST(REGEXP_REPLACE(TRIM(chavevant), r'\.0$', '') AS STRING) AS id_vantagem,
    SAFE_CAST(data_particao AS DATE) data_particao,
FROM {{ source('recursos_humanos_ergon', 'vantagens') }} AS t
WHERE
    SAFE_CAST(data_particao AS DATE) < CURRENT_DATE('America/Sao_Paulo')

{% if is_incremental() %}

{% set max_partition = run_query("SELECT gr FROM (SELECT IF(max(data_particao) > CURRENT_DATE('America/Sao_Paulo'), CURRENT_DATE('America/Sao_Paulo'), max(data_particao)) as gr FROM " ~ this ~ ")").columns[0].values()[0] %}

AND
    SAFE_CAST(data_particao AS DATE) > ("{{ max_partition }}")

{% endif %}