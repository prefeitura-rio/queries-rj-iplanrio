{{
    config(
        materialized='table',
        alias='fita_banco',
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
        tags=["raw", "ergon", "fita_banco", "salarios", "financeiro"],
        description="Fitas enviadas para o tesouro municipal contendo informações sobre pagamentos a serem feitos a funcionários."
    )
}}

SELECT
    SAFE_CAST(REGEXP_REPLACE(lancamento, r'\.0$', '') AS int64) AS id_lancamento,
    SAFE_CAST(mes_ano AS DATE) AS mes_ano_referencia,
    SAFE_CAST(dtexerc AS DATE) AS data_exercicio,
    SAFE_CAST(dtaposent AS DATE) AS data_aposentadoria,
    SAFE_CAST(dtvac AS DATE) AS data_vacancia,
    SAFE_CAST(data_credito AS DATE) AS data_credito,
    SAFE_CAST(REGEXP_REPLACE(numfunc, r'\.0$', '') AS int64) AS id_funcionario,
    SAFE_CAST(REGEXP_REPLACE(numvinc, r'\.0$', '') AS int64) AS id_vinculo,
    SAFE_CAST(REGEXP_REPLACE(numero, r'\.0$', '') AS INT64) AS numero_folha,
    SAFE_CAST(rubrica AS int64) AS id_rubrica,
    SAFE_CAST(setor AS int64) AS id_setor,
    SAFE_CAST(REGEXP_REPLACE(valorvan, r',', '.') AS FLOAT64) AS valor_vantagens,
    SAFE_CAST(REGEXP_REPLACE(valordes, r',', '.') AS FLOAT64) AS valor_descontos,
    SAFE_CAST(REGEXP_REPLACE(valorliq, r'\.0$', '') AS FLOAT64) AS valor_liquido,
    SAFE_CAST(REGEXP_REPLACE(numpens, r'\.0$', '') AS INT64) AS id_pensionista,
    SAFE_CAST(REGEXP_REPLACE(numdepen, r'\.0$', '') AS INT64) AS id_dependente,
    SAFE_CAST(REGEXP_REPLACE(banco, r'\.0$', '') AS INT64) AS banco,
    SAFE_CAST(REGEXP_REPLACE(agencia, r'\.0$', '') AS int64) AS agencia,
    SAFE_CAST(REGEXP_REPLACE(conta, r'\.0$', '') AS INT64) AS conta_banco,
    SAFE_CAST(REGEXP_REPLACE(cargo, r'\.0$', '') AS int64) AS id_cargo,
    SAFE_CAST(REGEXP_REPLACE(referencia, r'\.0$', '') AS STRING) AS referencia,
    SAFE_CAST(funcao AS STRING) AS funcao,
    SAFE_CAST(nome AS STRING) AS nome_funcionario,
    SAFE_CAST(REGEXP_REPLACE(emp_codigo, r'\.0$', '') AS int64) AS id_empresa,
    SAFE_CAST(REGEXP_REPLACE(ficha, r'\.0$', '') AS int64) AS id_ficha,
    SAFE_CAST(REGEXP_REPLACE(regimejur, r'\.0$', '') AS STRING) AS regime_juridico,
    SAFE_CAST(tipovinc AS STRING) AS tipo_vinculo,
    SAFE_CAST(subcategoria AS STRING) AS subcategoria,
    SAFE_CAST(categoria AS STRING) AS categoria,
    SAFE_CAST(REGEXP_REPLACE(cpf, r'\.0', '') AS STRING) AS cpf_funcionario,
    SAFE_CAST(REGEXP_REPLACE(flex_campo_05, r'\.0$', '') AS STRING) AS id_lotacao,
    SAFE_CAST(REGEXP_REPLACE(jornada, r'\.0$', '') AS STRING) AS jornada,
    SAFE_CAST(data_particao AS DATE) data_particao
FROM {{ source('brutos_ergon_staging', 'VW_DLK_ERG_FITA_BANCO') }} AS t