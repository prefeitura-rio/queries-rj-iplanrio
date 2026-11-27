{{
    config(
        materialized='table',
        alias='fita_banco',
        partition_by={
            "field": "updated_at",
            "data_type": "date",
            "granularity": "month",
        },
        tags=["raw", "ergon", "fita_banco", "salarios", "financeiro"],
        description="Fitas enviadas para o tesouro municipal contendo informações sobre pagamentos a serem feitos a funcionários."
    )
}}

SELECT
    SAFE_CAST (lancamento AS string) AS id_lancamento,
    SAFE_CAST (mes_ano AS DATE) AS mes_ano_referencia,
    SAFE_CAST (dtexerc AS DATE) AS data_exercicio,
    SAFE_CAST (dtaposent AS DATE) AS data_aposentadoria,
    SAFE_CAST (dtvac AS DATE) AS data_vacancia,
    SAFE_CAST (data_credito AS DATE) AS data_credito,
    SAFE_CAST (numfunc AS string) AS id_funcionario,
    SAFE_CAST (numvinc AS string) AS id_vinculo,
    SAFE_CAST (numero AS string) AS numero_folha,
    SAFE_CAST (rubrica AS string) AS id_rubrica,
    SAFE_CAST (setor AS string) AS id_setor,
    SAFE_CAST (valorvan AS FLOAT64) AS valor_vantagens,
    SAFE_CAST (valordes AS FLOAT64) AS valor_descontos,
    SAFE_CAST (valorliq AS FLOAT64) AS valor_liquido,
    SAFE_CAST (numpens AS string) AS id_pensionista,
    SAFE_CAST (numdepen AS string) AS id_dependente,
    SAFE_CAST (banco AS INT64) AS banco,
    SAFE_CAST (agencia AS int64) AS agencia,
    SAFE_CAST (conta AS INT64) AS conta_banco,
    SAFE_CAST (cargo AS string) AS id_cargo,
    SAFE_CAST (referencia AS STRING) AS referencia,
    SAFE_CAST (funcao AS STRING) AS funcao,
    SAFE_CAST (nome AS STRING) AS nome_funcionario,
    SAFE_CAST (emp_codigo AS string) AS id_empresa,
    SAFE_CAST (ficha AS string) AS id_ficha,
    SAFE_CAST (regimejur AS STRING) AS regime_juridico,
    SAFE_CAST (tipovinc AS STRING) AS tipo_vinculo,
    SAFE_CAST (subcategoria AS STRING) AS subcategoria,
    SAFE_CAST (categoria AS STRING) AS categoria,
    SAFE_CAST (cpf AS STRING) AS cpf_funcionario,
    SAFE_CAST (flex_campo_05 AS STRING) AS id_lotacao,
    SAFE_CAST (jornada AS STRING) AS jornada,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon', 'vw_dlk_erg_fita_banco') }} AS t