{{
    config(
        alias='lic_afast_gbp',
        materialized="table",
        tags=["raw", "ergon", "licenca", "afastamento"],
        description="Licenças e afastamentos dos servidores (view GBP)."
    )
}}

SELECT
    SAFE_CAST(l_dtini AS DATE) AS data_inicio,
    SAFE_CAST(l_dtfim AS DATE) AS data_final,
    SAFE_CAST(m7 AS STRING) AS m7,
    SAFE_CAST(codfreq AS STRING) AS id_afastamento,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon', 'vw_lic_afast_gbp') }} AS t
