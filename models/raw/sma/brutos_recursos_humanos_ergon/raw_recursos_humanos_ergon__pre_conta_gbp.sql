{{
    config(
        alias='pre_conta_gbp',
        materialized="table",
        tags=["raw", "ergon", "pre_contagem", "contagem"],
        description="Contagens anteriores ao início do sistema (view GBP)."
    )
}}

SELECT
    SAFE_CAST(m4 AS STRING) AS m4,
    SAFE_CAST(pre_contagem AS INT64) AS pre_contagem,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon', 'vw_pre_conta_gbp') }} AS t
