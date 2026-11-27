{{
    config(
        alias='tpmrj_pm_juntas',
        materialized="table",
        tags=["raw", "ergon", "pericia_medica", "tpmrj_pm_juntas"],
        description="Juntas médicas da perícia médica."
    )
}}

SELECT
    SAFE_CAST(id_junta AS STRING) AS id_junta,
    SAFE_CAST(numfunc AS STRING) AS id_funcionario,
    SAFE_CAST(sigla AS STRING) AS sigla,
    SAFE_CAST(dtini AS DATE) AS data_inicio,
    SAFE_CAST(justificativa AS STRING) AS justificativa,
    SAFE_CAST(dtconclusao AS DATE) AS data_conclusao,
    SAFE_CAST(numproc AS STRING) AS numero_processo,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon_pericia_medica', 'tpmrj_pm_juntas') }} AS t
