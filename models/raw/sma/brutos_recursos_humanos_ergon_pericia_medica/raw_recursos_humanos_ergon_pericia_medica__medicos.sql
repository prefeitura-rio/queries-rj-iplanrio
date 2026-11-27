{{
    config(
        alias='medicos',
        materialized="table",
        tags=["raw", "ergon", "pericia_medica", "medicos"],
        description="Médicos peritos."
    )
}}

SELECT
    SAFE_CAST(crm AS STRING) AS crm,
    SAFE_CAST(nome AS STRING) AS nome,
    SAFE_CAST(numfunc AS STRING) AS id_funcionario,
    SAFE_CAST(flex_campo_01 AS STRING) AS flex_campo_01,
    SAFE_CAST(flex_campo_02 AS STRING) AS flex_campo_02,
    SAFE_CAST(flex_campo_03 AS STRING) AS flex_campo_03,
    SAFE_CAST(flex_campo_04 AS STRING) AS flex_campo_04,
    SAFE_CAST(flex_campo_05 AS STRING) AS flex_campo_05,
    SAFE_CAST(datainativ AS DATE) AS data_inativacao,
    SAFE_CAST(id_pessoa AS STRING) AS id_pessoa,
    SAFE_CAST(flex_campo_06 AS STRING) AS flex_campo_06,
    SAFE_CAST(flex_campo_07 AS STRING) AS flex_campo_07,
    SAFE_CAST(flex_campo_08 AS STRING) AS flex_campo_08,
    SAFE_CAST(flex_campo_09 AS STRING) AS flex_campo_09,
    SAFE_CAST(flex_campo_10 AS STRING) AS flex_campo_10,
    SAFE_CAST(flex_campo_11 AS STRING) AS flex_campo_11,
    SAFE_CAST(flex_campo_12 AS STRING) AS flex_campo_12,
    SAFE_CAST(flex_campo_13 AS STRING) AS flex_campo_13,
    SAFE_CAST(flex_campo_14 AS STRING) AS flex_campo_14,
    SAFE_CAST(flex_campo_15 AS STRING) AS flex_campo_15,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon_pericia_medica', 'medicos') }} AS t
