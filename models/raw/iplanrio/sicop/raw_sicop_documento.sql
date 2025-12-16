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
    SAFE_CAST(SELECT (CASE WHEN LENGTH(DATA_DOCUMENTO) = 5 THEN NULL
    WHEN LENGTH(DATA_DOCUMENTO) = 6 AND SUBSTR(DATA_DOCUMENTO,6,1) = '0' THEN NULL
    WHEN LENGTH(DATA_DOCUMENTO) = 6 THEN CONCAT(SUBSTR(DATA_DOCUMENTO, 1,4 ),'-0',SUBSTR(DATA_DOCUMENTO, 5,1 ),'-0',SUBSTR(DATA_DOCUMENTO, 6,1 ))
    WHEN (LENGTH(DATA_DOCUMENTO) = 7 AND SUBSTR(DATA_DOCUMENTO,5,2) <'13') THEN CONCAT(SUBSTR(DATA_DOCUMENTO, 1,4 ),'-',SUBSTR(DATA_DOCUMENTO, 5,2 ),'-0',SUBSTR(DATA_DOCUMENTO, 7,1 ))
    WHEN (LENGTH(DATA_DOCUMENTO) = 7 AND SUBSTR(DATA_DOCUMENTO,5,2) >'12') THEN CONCAT(SUBSTR(DATA_DOCUMENTO, 1,4 ),'-0',SUBSTR(DATA_DOCUMENTO, 5,1 ),'-',SUBSTR(DATA_DOCUMENTO, 6,2 ))
    ELSE CONCAT(SUBSTR(DATA_DOCUMENTO,1,4),'-',SUBSTR(DATA_DOCUMENTO,5,2),'-',SUBSTR(DATA_DOCUMENTO,7,2)) END )  LIMIT 1  AS date) AS data_documento,
    
    SAFE_CAST(PRAZO_DOC AS INT64) AS prazo_documento,
    SAFE_CAST(PROC_JUD_DOC AS STRING) AS processo_judicial_documento,
    SAFE_CAST(NUMERO_EXTERNO AS STRING) AS numero_externo,
    SAFE_CAST(SUBSTR(_prefect_extracted_at,1,10) AS DATE) AS datalake_transformed_at
FROM {{ source('brutos_sicop_staging', 'documento') }}

