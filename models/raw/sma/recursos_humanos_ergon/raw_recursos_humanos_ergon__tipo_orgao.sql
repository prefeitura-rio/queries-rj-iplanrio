{{
    config(
        alias='tipo_orgao',
        materialized="table",
        tags=["raw", "ergon", "orgãos", "tipo_orgao"],
        description="Tabela que contém os registros dos tipos de órgãos da administração direta ou indireta da prefeitura do Rio de Janeiro."
    )
}}

SELECT
    SAFE_CAST(tipo AS STRING) AS tipo,
    SAFE_CAST(descr AS STRING) AS descricao
FROM {{ source('brutos_ergon_staging', 'VW_DLK_ERG_TIPO_ORGAO') }} AS t