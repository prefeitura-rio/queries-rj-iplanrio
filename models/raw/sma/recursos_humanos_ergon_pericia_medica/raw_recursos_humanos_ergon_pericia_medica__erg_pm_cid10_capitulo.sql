{{
    config(
        alias='erg_pm_cid10_capitulo'
    )
}}

SELECT safe_cast(CAPITULO AS STRING) AS capitulo, 
    safe_cast(DESCRICAO AS STRING) AS descricao, 
    safe_cast(FLEX_CAMPO_01 AS STRING) AS flex_campo_01,
    safe_cast(FLEX_CAMPO_02 AS STRING) AS flex_campo_02,
    safe_cast(FLEX_CAMPO_03 AS STRING) AS flex_campo_03,
    safe_cast(FLEX_CAMPO_04 AS STRING) AS flex_campo_04,
    safe_cast(FLEX_CAMPO_05 AS STRING) AS flex_campo_05,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at 
FROM {{ source('brutos_recursos_humanos_ergon_pericia_medica_staging', 'ERG_PM_CID10_CAPITULO') }}

