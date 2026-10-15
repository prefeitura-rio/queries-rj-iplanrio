{{
    config(
        alias='periodos_simples',
        description='Períodos das empresas no Simples Nacional'
    )
}}

SELECT
    SAFE_CAST(cnpj AS STRING) AS cnpj,
    SAFE_CAST(data_inicio AS STRING) AS data_inicio,
    SAFE_CAST(data_fim AS STRING) AS data_fim,
    SAFE_CAST(identificador_cancelamento AS STRING) AS identificador_cancelamento,
    SAFE_CAST(numero_opcao AS STRING) AS numero_opcao,
    SAFE_CAST(ano_particao AS INT64) AS ano_particao,
    SAFE_CAST(mes_particao AS INT64) AS mes_particao,
    SAFE_CAST(data_particao AS DATE) AS data_particao,
    SAFE_CAST(arquivo_origem AS STRING) AS arquivo_origem,
FROM {{ source('brutos_crf', 'periodos_simples') }}
