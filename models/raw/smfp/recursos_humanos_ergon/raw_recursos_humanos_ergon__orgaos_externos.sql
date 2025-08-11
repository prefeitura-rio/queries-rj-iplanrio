{{
    config(
        alias='raw_recursos_humanos_ergon__orgaos_externos',
    )
}}

SELECT
    SAFE_CAST(orgao AS STRING) AS nome,
    SAFE_CAST(descr AS STRING) AS nome_completo,
    SAFE_CAST(tipo_orgao AS STRING) AS tipo,
FROM {{ source('recursos_humanos_ergon', 'orgaos_externos') }} AS t