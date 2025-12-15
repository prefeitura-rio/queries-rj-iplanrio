{{
    config(
        alias="orgao",
        description="Dados brutos de órgãos do SICOP (VW_ORGAO_DLK)"
    )
}}

-- Conversões e padronização de nomes conforme guia de estilo
SELECT
    SAFE_CAST(ORG_SICOP AS STRING) AS id_orgao_sicop,
    SAFE_CAST(SIGLA_ORGAO AS STRING) AS sigla_orgao,
    SAFE_CAST(DESC_ORGAO AS STRING) AS descricao_orgao,
    SAFE_CAST(END_ORGAO AS STRING) AS endereco_orgao,
    SAFE_CAST(TEL_ORGAO AS STRING) AS telefone_orgao,
    SAFE_CAST(_prefect_extracted_at AS DATE) AS datalake_transformed_at
FROM {{ source('brutos_sicop_staging', 'orgao') }}

