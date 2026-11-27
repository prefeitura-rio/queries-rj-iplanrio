{{
    config(
        alias='erg_pm_pront_med',
        materialized="table",
        tags=["raw", "ergon", "pericia_medica", "erg_pm_pront_med"],
        description="Prontuários médicos da perícia médica."
    )
}}

SELECT
    SAFE_CAST(profispront AS STRING) AS id_profissional_prontuario,
    SAFE_CAST(chavepront AS STRING) AS id_prontuario,
    SAFE_CAST(crm AS STRING) AS crm,
    SAFE_CAST(profissional AS STRING) AS id_profissional,
    SAFE_CAST(senha AS string) as string,
    SAFE_CAST(ok AS STRING) AS ok,
    SAFE_CAST(nok AS STRING) AS nok,
    SAFE_CAST(justificativa AS STRING) AS justificativa,
    SAFE_CAST(data AS DATE) AS data,
    SAFE_CAST(ordem_homol AS INT64) AS ordem_homologacao,
    SAFE_CAST(flex_campo_01 AS STRING) AS flex_campo_01,
    SAFE_CAST(flex_campo_02 AS STRING) AS flex_campo_02,
    SAFE_CAST(flex_campo_03 AS STRING) AS flex_campo_03,
    SAFE_CAST(flex_campo_04 AS STRING) AS flex_campo_04,
    SAFE_CAST(flex_campo_05 AS STRING) AS flex_campo_05,
    SAFE_CAST(flex_campo_06 AS STRING) AS flex_campo_06,
    SAFE_CAST(flex_campo_07 AS STRING) AS flex_campo_07,
    SAFE_CAST(flex_campo_08 AS STRING) AS flex_campo_08,
    SAFE_CAST(flex_campo_09 AS STRING) AS flex_campo_09,
    SAFE_CAST(flex_campo_10 AS STRING) AS flex_campo_10,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon_pericia_medica', 'erg_pm_pront_med') }} AS t
