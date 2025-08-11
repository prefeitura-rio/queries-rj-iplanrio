{{
    config(
        alias='raw_recursos_humanos_ergon__tipo_vinculo',
    )
}}

SELECT
    SAFE_CAST(sigla AS STRING) AS sigla,
    SAFE_CAST(nome AS STRING) AS nome,
FROM {{ source('recursos_humanos_ergon', 'tipo_vinculo') }} AS t