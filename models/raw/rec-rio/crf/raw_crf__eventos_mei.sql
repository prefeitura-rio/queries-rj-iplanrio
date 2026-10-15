{{
    config(
        alias='eventos_mei',
        description='Eventos das empresas no SIMEI'
    )
}}

SELECT
    SAFE_CAST(cnpj AS STRING) AS cnpj,
    SAFE_CAST(natureza_evento AS STRING) AS natureza_evento,
    SAFE_CAST(codigo_evento AS STRING) AS codigo_evento,

    SAFE.PARSE_DATE("%Y%m%d", data_fato_motivador) AS data_fato_motivador,
    SAFE.PARSE_DATE("%Y%m%d", data_efeito) AS data_efeito,

    SAFE_CAST(numero_processo_judicial AS STRING) AS numero_processo_judicial,
    SAFE_CAST(numero_processo_administrativo AS STRING) AS numero_processo_administrativo,
    SAFE_CAST(observacoes AS STRING) AS observacoes,
    SAFE_CAST(codigo_ua AS INT64) AS codigo_ua,
    SAFE_CAST(codigo_uf AS STRING) AS codigo_uf,
    SAFE_CAST(codigo_municipio AS INT64) AS codigo_municipio,

    SAFE.PARSE_DATE("%Y%m%d", data_ocorrencia) AS data_ocorrencia,
    SAFE.PARSE_TIME("%H%M%S", hora_ocorrencia) AS hora_ocorrencia,

    SAFE_CAST(numero_opcao AS STRING) AS numero_opcao,
    SAFE_CAST(ano_particao AS INT64) AS ano_particao,
    SAFE_CAST(mes_particao AS INT64) AS mes_particao,
    SAFE_CAST(data_particao AS DATE) AS data_particao,
    SAFE_CAST(arquivo_origem AS STRING) AS arquivo_origem
FROM {{ source('brutos_crf', 'eventos_mei') }}
