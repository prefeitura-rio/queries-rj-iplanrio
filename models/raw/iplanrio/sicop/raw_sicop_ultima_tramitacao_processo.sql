{{
    config(
        alias="ultima_tramitacao_processo",
        description="Dados brutos da última tramitação de processos do SICOP (VW_ULTIMA_TRAM_PROCESSO_DLK)"
    )
}}

-- Conversões e padronização de nomes conforme guia de estilo
SELECT
    SAFE_CAST(I22004_COD_NPROC2 AS STRING) AS codigo_processo,
    SAFE_CAST(I22004_RA_NPROC2 AS STRING) AS ra_processo,
    SAFE_CAST(I22004_SEQ_NPROC2 AS INT64) AS sequencia_processo,
    SAFE_CAST(I22004_SEC_NPROC2 AS STRING) AS secao_processo,
    SAFE_CAST(I22004_ANO_NPROC2 AS INT64) AS ano_processo,
    SAFE_CAST(MAX_SEQ AS INT64) AS sequencia_maxima,
    SAFE_CAST(I22004_ORG_ORIGEM AS STRING) AS id_orgao_origem,
    SAFE_CAST(I22004_ORG_DEST AS STRING) AS id_orgao_destino,
    SAFE_CAST(I22004_COD_DESP AS STRING) AS codigo_despacho,
    SAFE_CAST(I22004_MAT_TRANSC AS STRING) AS matricula_transcritor,
    SAFE_CAST(I22004_NUM_GUIA AS STRING) AS numero_guia,
    SAFE_CAST(_prefect_extracted_at AS DATE) AS datalake_transformed_at
FROM {{ source('brutos_sicop_staging', 'ultima_tram_processo') }}

