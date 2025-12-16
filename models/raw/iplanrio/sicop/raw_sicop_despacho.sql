{{
    config(
        alias="despacho",
        description="Dados brutos de despachos do SICOP (VW_DESPACHO_DLK)"
    )
}}

-- Conversões e padronização de nomes conforme guia de estilo
SELECT
    SAFE_CAST(IDENT AS STRING) AS id_despacho,
    SAFE_CAST(SEQ_REM AS INT64) AS sequencia_remissao,
    SAFE_CAST(SEQ AS INT64) AS sequencia,
    SAFE_CAST(DESCRICAO AS STRING) AS descricao,
    SAFE_CAST(_prefect_extracted_at AS DATE) AS datalake_transformed_at
FROM {{ source('brutos_sicop_staging', 'despacho') }}

