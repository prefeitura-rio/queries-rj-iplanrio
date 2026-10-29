{{
    config(
        alias='periodos_simples',
        description='Períodos das empresas no Simples Nacional'
    )
}}

SELECT
    SAFE_CAST(cnpj AS STRING) AS cnpj,
    SAFE.PARSE_DATE("%Y%m%d",data_inicio) AS data_inicio,
    SAFE.PARSE_DATE("%Y%m%d",data_fim) AS data_fim,
    SAFE_CAST(identificador_cancelamento AS STRING) AS identificador_cancelamento,
    SAFE_CAST(numero_opcao AS STRING) AS numero_opcao,
    SAFE_CAST(ano_particao AS INT64) AS ano_particao,
    SAFE_CAST(mes_particao AS INT64) AS mes_particao,
    SAFE_CAST(data_particao AS DATE) AS data_particao,
    SAFE_CAST(arquivo_origem AS STRING) AS arquivo_origem,
FROM {{ source('brutos_crf', 'periodos_simples') }}
