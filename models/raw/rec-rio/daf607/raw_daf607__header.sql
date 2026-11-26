{{
    config(
        alias='daf607_header',
        materialized='table',
        description='Registro header do arquivo DAF607 - Informações do SERPRO relativas às guias do Simples Nacional'
    )
}}
    SELECT
    SAFE_CAST(codigo_registro AS STRING) AS codigo_registro,
    SAFE_CAST(sequencial AS STRING) AS sequencial,
    SAFE_CAST(codigo_convenio AS STRING) AS codigo_convenio,
    SAFE.PARSE_DATE("%Y%m%d", data_geracao) AS data_geracao,
    SAFE_CAST(numero_remessa AS INT64) AS numero_remessa,
    SAFE_CAST(versao AS INT64) AS versao,
    SAFE_CAST(banco_arrecadador AS STRING) AS banco_arrecadador,
    SAFE.PARSE_DATE("%Y%m%d", data_credito) AS data_credito,

    SAFE_CAST(ano_particao AS INT64) AS ano_particao,
    SAFE_CAST(mes_particao AS INT64) AS mes_particao,
    SAFE_CAST(data_particao AS DATE) AS data_particao,
    SAFE_CAST(arquivo_origem AS STRING) AS arquivo_origem,
        SAFE_CAST(loaded_at as DATETIME) AS loaded_at
FROM {{ source('brutos_crf', 'daf607_header') }}
