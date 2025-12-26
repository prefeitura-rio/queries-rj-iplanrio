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
    SAFE_CAST(CONCAT( SUBSTR(DATA_DOCUMENTO,7,4),'-', SUBSTR(DATA_DOCUMENTO,4,2) ,'-', SUBSTR(DATA_DOCUMENTO,1,2) ) as date)         as data_documento,
    SAFE_CAST(PRAZO_DOC AS INT64) AS prazo_documento,
    SAFE_CAST(PROC_JUD_DOC AS STRING) AS processo_judicial_documento,
    SAFE_CAST(NUMERO_EXTERNO AS STRING) AS numero_externo,  
    SAFE_CAST(NUM_PROCESSO AS STRING) AS numero_processo,
    SAFE_CAST(REQUERENTE AS STRING) AS requerente,
    SAFE_CAST(COD_ASSUN AS STRING) AS codigo_assunto,
    SAFE_CAST(ASSUN_COMP AS STRING) AS assunto_completo,
    SAFE_CAST(TP_DOC_INF AS STRING) AS tipo_documento_informado,
    SAFE_CAST(NUM_DOCUMENTO_INF AS STRING) AS numero_documento_informado,
    SAFE_CAST(ORG_RESP_INF AS STRING) AS id_orgao_responsavel_informado,
    SAFE_CAST(SUBSTR(_prefect_extracted_at,1,10) AS DATE) AS datalake_transformed_at
FROM {{ source('brutos_sicop_staging', 'documento') }}

