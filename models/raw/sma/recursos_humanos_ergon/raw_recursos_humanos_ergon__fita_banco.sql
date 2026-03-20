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
    {{ clean_and_cast('lancamento', 'int64') }} AS id_lancamento,
    SAFE_CAST(mes_ano AS DATE) AS mes_ano_referencia,
    SAFE_CAST(dtexerc AS DATE) AS data_exercicio,
    SAFE_CAST(dtaposent AS DATE) AS data_aposentadoria,
    SAFE_CAST(dtvac AS DATE) AS data_vacancia,
    SAFE_CAST(data_credito AS DATE) AS data_credito,
    {{ clean_and_cast('numfunc', 'int64') }} AS id_funcionario,
    {{ clean_and_cast('numvinc', 'int64') }} AS id_vinculo,
    {{ clean_and_cast('numero', 'int64') }} AS numero_folha,
    SAFE_CAST(rubrica AS int64) AS id_rubrica,
    SAFE_CAST(setor AS int64) AS id_setor,
    SAFE_CAST(REGEXP_REPLACE(valorvan, r',', '.') AS FLOAT64) AS valor_vantagens,
    SAFE_CAST(REGEXP_REPLACE(valordes, r',', '.') AS FLOAT64) AS valor_descontos,
    {{ clean_and_cast('valorliq', 'float64') }} AS valor_liquido,
    {{ clean_and_cast('numpens', 'int64') }} AS id_pensionista,
    {{ clean_and_cast('numdepen', 'int64') }} AS id_dependente,
    {{ clean_and_cast('banco', 'int64') }} AS banco,
    {{ clean_and_cast('agencia', 'int64') }} AS agencia,
    {{ clean_and_cast('conta', 'int64') }} AS conta_banco,
    {{ clean_and_cast('cargo', 'int64') }} AS id_cargo,
    {{ clean_and_cast('referencia', 'string') }} AS referencia,
    SAFE_CAST(funcao AS STRING) AS funcao,
    SAFE_CAST(nome AS STRING) AS nome_funcionario,
    {{ clean_and_cast('emp_codigo', 'int64') }} AS id_empresa,
    {{ clean_and_cast('ficha', 'int64') }} AS id_ficha,
    {{ clean_and_cast('regimejur', 'string') }} AS regime_juridico,
    SAFE_CAST(tipovinc AS STRING) AS tipo_vinculo,
    SAFE_CAST(subcategoria AS STRING) AS subcategoria,
    SAFE_CAST(categoria AS STRING) AS categoria,
    SAFE_CAST(REGEXP_REPLACE(cpf, r'\.0', '') AS STRING) AS cpf_funcionario,
    {{ clean_and_cast('flex_campo_05', 'string') }} AS id_lotacao,
    {{ clean_and_cast('jornada', 'string') }} AS jornada,
    SAFE_CAST(data_particao AS DATE) data_particao
FROM {{ source('brutos_ergon_staging', 'VW_DLK_ERG_FITA_BANCO') }} AS t