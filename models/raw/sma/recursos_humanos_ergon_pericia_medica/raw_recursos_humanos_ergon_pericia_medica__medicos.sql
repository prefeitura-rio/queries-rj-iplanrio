{{
    config(
        alias='medicos'
    )
}}

SELECT safe_cast(CRM AS STRING) AS crm, 
safe_cast(NOME AS STRING) AS nome,
safe_cast(NUMFUNC AS STRING) AS numfunc,
safe_cast(ID_PESSOA AS STRING) AS id_pessoa,
safe_cast(DATAINATIV AS DATE) AS datainativ,
safe_cast(FLEX_CAMPO_01 AS STRING) AS flex_campo_01,
safe_cast(FLEX_CAMPO_02 AS STRING) AS flex_campo_02,
safe_cast(FLEX_CAMPO_03 AS STRING) AS flex_campo_03,
safe_cast(FLEX_CAMPO_04 AS STRING) AS flex_campo_04,
safe_cast(FLEX_CAMPO_05 AS STRING) AS flex_campo_05,
safe_cast(FLEX_CAMPO_06 AS STRING) AS flex_campo_06,
safe_cast(FLEX_CAMPO_07 AS STRING) AS flex_campo_07,
safe_cast(FLEX_CAMPO_08 AS STRING) AS flex_campo_08,
safe_cast(FLEX_CAMPO_09 AS STRING) AS flex_campo_09,
safe_cast(FLEX_CAMPO_10 AS STRING) AS flex_campo_10,
safe_cast(FLEX_CAMPO_11 AS STRING) AS flex_campo_11,
safe_cast(FLEX_CAMPO_12 AS STRING) AS flex_campo_12,
safe_cast(FLEX_CAMPO_13 AS STRING) AS flex_campo_13,
safe_cast(FLEX_CAMPO_14 AS STRING) AS flex_campo_14,    
safe_cast(FLEX_CAMPO_15 AS STRING) AS flex_campo_15,
_airbyte_extracted_at as datalake_loaded_at, 
current_timestamp() as datalake_transformed_at 
FROM {{ source('brutos_recursos_humanos_ergon_pericia_medica_staging', 'MEDICOS') }}

