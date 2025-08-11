{{
    config(
        alias='raw_recursos_humanos_ergon__regime_juridico',
    )
}}

SELECT
    SAFE_CAST(sigla AS STRING) AS sigla,
    SAFE_CAST(nome AS STRING) AS nome,
FROM {{ source('recursos_humanos_ergon', 'regime_juridico') }} AS t