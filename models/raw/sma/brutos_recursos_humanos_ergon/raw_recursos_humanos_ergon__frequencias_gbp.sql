{{
    config(
        alias='frequencias_gbp',
        materialized="table",
        tags=["raw", "ergon", "frequencia"],
        description="Tabela que armazena as ocorrências de frequência do servidor municipal (view GBP)."
    )
}}

SELECT
    SAFE_CAST(fr_dtini AS DATE) AS data_inicio,
    SAFE_CAST(fr_dtfim AS DATE) AS data_final,
    SAFE_CAST(m6 AS STRING) AS m6,
    SAFE_CAST(codfreq AS STRING) AS id_frequencia,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon', 'vw_frequencias_gbp') }} AS t
