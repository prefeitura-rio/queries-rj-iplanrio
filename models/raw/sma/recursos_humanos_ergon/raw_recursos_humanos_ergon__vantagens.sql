
{{
    config(
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
        alias='vantagens',
        materialized="table",
        tags=["raw", "ergon", "vantagens"],
        description="Tabela que armazena informações sobre atributos cadastrados que representam vantagens (valores a receber) dos servidores."
    )
}}

SELECT
    safe_cast(numfunc AS int64) AS id_funcionario,
    safe_cast(numvinc AS int64) AS id_vinculo,
    safe_cast(vantagem AS string) AS nome_vantagem,
    safe_cast(trim(substr(dtini, 1, 10)) AS DATE) AS data_inicio,
    safe_cast(trim(substr(dtfim,1 , 10)) AS DATE) AS data_final,
    safe_cast(valor as numeric) AS valor_vantagem,
    safe_cast(info AS string) AS informacao_atributo,
    safe_cast(tipo_incorporacao AS string) AS tipo_incorporacao_cargo_fiducia,
    safe_cast(perc_inc_funcao AS numeric) AS percentual_incorporacao_cargo_fiducia,
    safe_cast(inc_tabelavenc AS string) AS incide_tabela_vencimentos,
    safe_cast(inc_referencia AS string) AS incide_tabela_simbolo,
    safe_cast(obs AS string) AS observacoes,
    safe_cast(valor2 AS numeric) AS valor_complementar_1,
    safe_cast(info2 AS string) AS informacao_complementar_1,
    safe_cast(valor3 AS numeric) AS valor_complementar_2,
    safe_cast(info3 AS string) AS informacao_complementar_2,
    safe_cast(valor4 AS numeric) AS valor_coplementar_3,
    safe_cast(info4 AS string) AS informacao_complementar_3,
    safe_cast(valor5 AS numeric) AS valor_complementar_4,
    safe_cast(info5 AS string) AS informacao_complementar_4,
    safe_cast(valor6 AS numeric) AS valor_complementar_5,
    safe_cast(info6 AS string) AS informacao_complementar_5,
    safe_cast(replace(flex_campo_05, ',', '.') AS numeric) AS valor_incorporado,
    safe_cast(emp_codigo AS int64) AS id_empresa,
    safe_cast(chavevant AS int64) AS id_vantagem,
    safe_cast(data_particao AS DATE) data_particao
FROM {{ source('brutos_ergon_staging', 'VANTAGENS') }} AS t
