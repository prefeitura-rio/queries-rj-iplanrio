{{
    config(
        alias='daf607_trailer',
        materialized='table',
        description='Registro trailer do arquivo DAF607 - Informações do SERPRO relativas às guias do Simples Nacional'
    )
}}

SELECT
    SAFE_CAST(codigo_registro AS STRING) AS codigo_registro,
    SAFE_CAST(sequencial AS STRING) AS sequencial,
    SAFE_CAST(total_registros AS INT64) AS total_registros,
    SAFE_CAST(valor_total_recebido AS FLOAT64) AS valor_total_recebido,
    SAFE_CAST(ano_particao AS INT64) AS ano_particao,
    SAFE_CAST(mes_particao AS INT64) AS mes_particao,
    SAFE_CAST(data_particao AS DATE) AS data_particao,
    SAFE_CAST(arquivo_origem AS STRING) AS arquivo_origem,
        SAFE_CAST(loaded_at as DATETIME) AS loaded_at

FROM {{ source('brutos_crf', 'daf607_trailer') }}
