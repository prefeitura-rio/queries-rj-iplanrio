{{
    config(
        alias="documento",
        description="Dados brutos de documentos do SICOP"
    )
}}

-- Conversões e padronização de nomes conforme guia de estilo
SELECT
    SAFE_CAST(TIPO_DOC AS STRING) AS tipo_documento,
    SAFE_CAST(NUM_DOCUMENTO AS STRING) AS numero_documento,
    SAFE_CAST(ORG_RESP AS STRING) AS id_orgao_responsavel,
    SAFE_CAST(DATA_DOCUMENTO AS DATE) AS data_documento,
    SAFE_CAST(PRAZO_DOC AS INT64) AS prazo_documento,
    SAFE_CAST(PROC_JUD_DOC AS STRING) AS processo_judicial_documento,
    SAFE_CAST(NUMERO_EXTERNO AS STRING) AS numero_externo,
    SAFE_CAST(_prefect_extracted_at AS DATE) AS datalake_transformed_at
FROM {{ source('brutos_sicop_staging', 'documento') }}

