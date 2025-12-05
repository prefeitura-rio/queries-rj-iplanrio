{{
    config(
        alias='erg_pm_grupoexame'
        
    )
}}

SELECT safe_cast(NOME AS STRING) AS nome, 
    safe_cast(GRUPOEXAME AS STRING) AS grupoexame,
    safe_cast(FLEX_CAMPO_01 AS STRING) AS flex_campo_01, 
    safe_cast(FLEX_CAMPO_02 AS STRING) AS flex_campo_02, 
    safe_cast(FLEX_CAMPO_03 AS STRING) AS flex_campo_03, 
    safe_cast(FLEX_CAMPO_04 AS STRING) AS flex_campo_04, 
    safe_cast(FLEX_CAMPO_05 AS STRING) AS flex_campo_05 
FROM {{ source('brutos_recursos_humanos_ergon_pericia_medica_staging', 'ERG_PM_GRUPOEXAME') }}

