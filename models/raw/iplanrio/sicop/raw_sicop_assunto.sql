{{
    config(
        alias="assunto",
        description="Dados brutos de assuntos do SICOP (VW_ASSUNTO_DLK)"
    )
}}

-- Conversões e padronização de nomes conforme guia de estilo
SELECT
    SAFE_CAST(IDENT AS STRING) AS id_assunto,
    SAFE_CAST(COD AS STRING) AS codigo_assunto,
    SAFE_CAST(SEQ AS INT64) AS sequencia,
    SAFE_CAST(CHAVE_IDENT_COD_SEQ AS STRING) AS chave_ident_cod_seq,
    SAFE_CAST(DESC_ASSUNTO AS STRING) AS descricao_assunto,
    SAFE_CAST(SUBSTR(_prefect_extracted_at,1,10) AS DATE) AS datalake_transformed_at
FROM {{ source('brutos_sicop_staging', 'assunto') }}

