{{
    config(
        alias='pessoa',
        schema='adm_central_atendimento_1746',
        materialized='table',
        unique_key='id_pessoa',
    )
}}

SELECT
    SAFE_CAST(
        REGEXP_REPLACE(id_pessoa, r'\.0$', '') AS INT64
    ) id_pessoa,
    SAFE_CAST(
        REGEXP_REPLACE(ds_cpf, r'\.0$', '') AS STRING
    ) cpf,
    SAFE_CAST(
        REGEXP_REPLACE(no_pessoa, r'\.0$', '') AS STRING
    ) nome,
    SAFE_CAST(
        REGEXP_REPLACE(ds_email, r'\.0$', '') AS STRING
    ) email,
    SAFE_CAST(
        REGEXP_REPLACE(ds_endereco, r'\.0$', '') AS STRING
    ) endereco,
    SAFE_CAST(
        SAFE_CAST(ds_endereco_numero AS FLOAT64) AS INT64
    ) endereco_numero,
    SAFE_CAST(
        REGEXP_REPLACE(ds_endereco_cep, r'\.0$', '') AS STRING
    ) endereco_cep,
    SAFE_CAST(
        REGEXP_REPLACE(ds_endereco_complemento, r'\.0$', '') AS STRING
    ) endereco_complemento,
    SAFE_CAST(
        REGEXP_REPLACE(ds_endereco_referencia, r'\.0$', '') AS STRING
    ) endereco_referencia,
    SAFE_CAST(
        REGEXP_REPLACE(ds_telefone_1, r'\.0$', '') AS STRING
    ) telefone_1,
    SAFE_CAST(
        REGEXP_REPLACE(ds_telefone_2, r'\.0$', '') AS STRING
    ) telefone_2,
    SAFE_CAST(
        REGEXP_REPLACE(ds_telefone_3, r'\.0$', '') AS STRING
    ) telefone_3,
    SAFE_CAST(
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d', dt_nascimento) AS DATE -- TODO: review date format
    ) data_nascimento,
    SAFE_CAST(
        REGEXP_REPLACE(ic_sexo, r'\.0$', '') AS STRING
    ) sexo,
    SAFE_CAST(
        REGEXP_REPLACE(ds_identidade, r'\.0$', '') AS STRING
    ) documento_identidade,
    SAFE_CAST(
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', dt_insercao) AS DATETIME -- TODO: review timestamp format
    ) data_insercao,
    SAFE_CAST(
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', dt_atualizacao) AS DATETIME -- TODO: review timestamp format
    ) data_atualizacao,
    SAFE_CAST(
        REGEXP_REPLACE(no_mae, r'\.0$', '') AS STRING
    ) nome_mae,
    SAFE_CAST(
        REGEXP_REPLACE(id_escolaridade_fk, r'\.0$', '') AS INT64
    ) id_escolaridade,
    SAFE_CAST(
        REGEXP_REPLACE(ds_atividade_profissional, r'\.0$', '') AS STRING
    ) atividade_profissional
FROM {{ source('brutos_1746_staging', 'pessoa') }} AS t