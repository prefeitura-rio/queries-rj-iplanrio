{{
    config(
        alias='daf607_detalhe',
        materialized='table',
        description='Registro detalhe do arquivo DAF607 - Informações do SERPRO relativas às guias do Simples Nacional'
    )
}}

SELECT
    SAFE_CAST(codigo_registro AS STRING) AS codigo_registro,
    SAFE_CAST(sequencial AS STRING) AS sequencial,
    SAFE.PARSE_DATE("%Y%m%d", data_arrecadacao) AS data_arrecadacao,
    SAFE.PARSE_DATE("%Y%m%d", data_vencimento) AS data_vencimento,
    SAFE_CAST(cnpj_contribuinte AS STRING) AS cnpj_contribuinte,
    SAFE_CAST(esfera AS STRING) AS esfera,
    SAFE_CAST(competencia AS STRING) AS competencia,
    SAFE_CAST(valor_principal AS FLOAT64) AS valor_principal,
    SAFE_CAST(valor_multa AS FLOAT64) AS valor_multa,
    SAFE_CAST(valor_juros AS FLOAT64) AS valor_juros,
    SAFE_CAST(valor_autenticacao AS FLOAT64) AS valor_autenticacao,
    SAFE_CAST(numero_autenticacao_banco AS STRING) AS numero_autenticacao_banco,
    SAFE_CAST(banco_arrecadador AS STRING) AS banco_arrecadador,
    SAFE_CAST(agencia_arrecadadora AS STRING) AS agencia_arrecadadora,
    SAFE_CAST(codigo_siafi AS INT64) AS codigo_siafi,
    SAFE_CAST(identificador_serpro AS STRING) AS identificador_serpro,
    SAFE_CAST(ano_particao AS INT64) AS ano_particao,
    SAFE_CAST(mes_particao AS INT64) AS mes_particao,
    SAFE_CAST(data_particao AS DATE) AS data_particao,
    SAFE_CAST(arquivo_origem AS STRING) AS arquivo_origem,
        SAFE_CAST(loaded_at as DATETIME) AS loaded_at

FROM {{ source('brutos_crf', 'daf607_detalhe') }}
