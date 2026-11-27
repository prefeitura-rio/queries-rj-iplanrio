{{
    config(
        alias='erg_pm_cid10_capitulo',
        materialized="table",
        tags=["raw", "ergon", "pericia_medica", "erg_pm_cid10_capitulo"],
        description="Capítulos da CID-10 usados na perícia médica."
    )
}}

SELECT
    SAFE_CAST(capitulo AS STRING) AS capitulo,
    SAFE_CAST(descricao AS STRING) AS descricao,
    SAFE_CAST(flex_campo_01 AS STRING) AS flex_campo_01,
    SAFE_CAST(flex_campo_02 AS STRING) AS flex_campo_02,
    SAFE_CAST(flex_campo_03 AS STRING) AS flex_campo_03,
    SAFE_CAST(flex_campo_04 AS STRING) AS flex_campo_04,
    SAFE_CAST(flex_campo_05 AS STRING) AS flex_campo_05,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon_pericia_medica', 'erg_pm_cid10_capitulo') }} AS t
