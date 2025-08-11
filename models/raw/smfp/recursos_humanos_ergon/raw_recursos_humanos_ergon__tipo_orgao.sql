{{
    config(
        alias='raw_recursos_humanos_ergon__tipo_orgao',
    )
}}

SELECT
    SAFE_CAST(tipo AS STRING) AS tipo,
    SAFE_CAST(descr AS STRING) AS descricao,
FROM {{ source('recursos_humanos_ergon', 'tipo_orgao') }} AS t