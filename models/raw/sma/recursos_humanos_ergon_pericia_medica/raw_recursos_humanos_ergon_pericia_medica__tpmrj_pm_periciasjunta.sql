{{
    config(
        alias='tpmrj_pm_periciasjunta'
    )
}}

SELECT safe_cast(ID_JUNTA AS STRING) AS id_junta, 
safe_cast(CHAVEPRONT AS STRING) AS chave_prontuario, 
safe_cast(ID_PERICIA_JUNTA AS STRING) AS id_pericia_junta,
_airbyte_extracted_at as datalake_loaded_at, 
current_timestamp() as datalake_transformed_at  
FROM {{ source('brutos_recursos_humanos_ergon_pericia_medica_staging', 'TPMRJ_PM_PERICIASJUNTA') }}

