{{
    config(
        alias='tpmrj_pm_juntas'
    )
}}

SELECT safe_cast(DTINI AS DATE) AS dt_inicio, 
    safe_cast(SIGLA AS STRING) AS sigla, 
    safe_cast(NUMFUNC AS STRING) AS num_func, 
    safe_cast(NUMPROC AS STRING) AS num_proc, 
    safe_cast(ID_JUNTA AS STRING) AS id_junta, 
    safe_cast(DTCONCLUSAO AS DATE) AS dt_conclusao, 
    safe_cast(JUSTIFICATIVA AS STRING) AS justificativa,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at  
    FROM {{ source('brutos_recursos_humanos_ergon_pericia_medica_staging', 'TPMRJ_PM_JUNTAS') }}

