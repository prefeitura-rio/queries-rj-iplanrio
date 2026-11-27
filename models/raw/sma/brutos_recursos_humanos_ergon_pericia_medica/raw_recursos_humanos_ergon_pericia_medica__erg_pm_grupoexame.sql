{{
    config(
        alias='erg_pm_grupoexame',
        materialized="table",
        tags=["raw", "ergon", "pericia_medica", "erg_pm_grupoexame"],
        description="Grupos de exames da perícia médica."
    )
}}

SELECT
    SAFE_CAST(grupoexame AS STRING) AS id_grupo_exame,
    SAFE_CAST(nome AS STRING) AS nome,
    SAFE_CAST(flex_campo_01 AS STRING) AS flex_campo_01,
    SAFE_CAST(flex_campo_02 AS STRING) AS flex_campo_02,
    SAFE_CAST(flex_campo_03 AS STRING) AS flex_campo_03,
    SAFE_CAST(flex_campo_04 AS STRING) AS flex_campo_04,
    SAFE_CAST(flex_campo_05 AS STRING) AS flex_campo_05,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon_pericia_medica', 'erg_pm_grupoexame') }} AS t
