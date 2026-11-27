{{
    config(
        alias='tpmrj_pm_periciasjunta',
        materialized="table",
        tags=["raw", "ergon", "pericia_medica", "tpmrj_pm_periciasjunta"],
        description="Perícias realizadas pelas juntas médicas."
    )
}}

SELECT
    SAFE_CAST(id_pericia_junta AS STRING) AS id_pericia_junta,
    SAFE_CAST(id_junta AS STRING) AS id_junta,
    SAFE_CAST(chavepront AS STRING) AS id_prontuario,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon_pericia_medica', 'tpmrj_pm_periciasjunta') }} AS t
