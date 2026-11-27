{{
    config(
        alias='vantagens_gbp',
        materialized="table",
        tags=["raw", "ergon", "vantagens"],
        description="Vantagens e atributos dos servidores (view GBP)."
    )
}}

SELECT
    SAFE_CAST(m2 AS STRING) AS m2,
    SAFE_CAST(inicio_abono AS DATE) AS data_inicio_abono,
    SAFE_CAST(abono AS STRING) AS abono,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon', 'vw_vantagens_gbp') }} AS t
