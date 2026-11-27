{{
    config(
        alias='categoria',
        materialized="table",
        tags=["raw", "ergon", "categoria"],
        description="Categorias existentes na administração direta ou indireta da PCRJ."
    )
}}

SELECT
    SAFE_CAST(sigla AS STRING) AS sigla,
    SAFE_CAST(nome AS STRING) AS nome,
    SAFE_CAST(pontlei AS INT64) AS pontuacao_lei,
    SAFE_CAST(pontpubl AS INT64) AS pontuacao_publicacao,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon', 'vw_dlk_erg_categorias_') }} AS t
