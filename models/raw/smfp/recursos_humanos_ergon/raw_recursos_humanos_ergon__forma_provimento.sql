{{
    config(
        alias='raw_recursos_humanos_ergon__forma_provimento',
    )
}}

SELECT
    SAFE_CAST(sigla AS STRING) AS sigla,
    SAFE_CAST(nome AS STRING) AS nome,
    SAFE_CAST(inativo AS STRING) AS inativo,
    SAFE_CAST(primeiro_prov AS STRING) AS primeiro_provimento,
    SAFE_CAST(ativo AS STRING) AS ativo,
FROM {{ source('recursos_humanos_ergon', 'forma_provimento') }} AS t