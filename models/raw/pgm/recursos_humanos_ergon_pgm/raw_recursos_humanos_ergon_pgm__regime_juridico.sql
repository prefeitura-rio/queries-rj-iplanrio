{{
    config(
        alias='regime_juridico',
        materialized="table",
        tags=["raw", "ergon", "regime_juridico"],
        description="Tipos de regimes jurídicos existentes entre os vínculos de funcionários e a administração direta ou indireta da prefeitura do Rio de Janeiro."
    )
}}

SELECT
    sigla,
    nome
FROM {{ ref('raw_recursos_humanos_ergon__regime_juridico') }} AS t