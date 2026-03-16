{{
    config(
        alias='tipo_vinculo',
        materialized="table",
        tags=["raw", "ergon", "tipo", "tipo_vinculo"],
        description="Tipos de vínculos funcionais existentes tanto na administração direta como indireta da prefeitura do Rio de Janeiro."
    )
}}

SELECT
    SAFE_CAST(sigla AS STRING) AS sigla,
    SAFE_CAST(nome AS STRING) AS nome
FROM {{ source('brutos_ergon_staging', 'VW_DLK_ERG_TIPO_VINC_') }} AS t