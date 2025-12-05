{{
    config(
        alias='erg_pm_pront_med'
    )
}}

SELECT safe_cast(OK AS STRING) AS ok,
    safe_cast(CRM AS STRING) AS crm,
    safe_cast(NOK AS STRING) AS nok,
    safe_cast(DATA AS DATE) AS data_atualizacao,
    safe_cast(SENHA AS STRING) AS senha,
    safe_cast(CHAVEPRONT AS STRING) AS chave_prontuario,
    safe_cast(ORDEM_HOMOL AS STRING) AS ordem_homolog,
    safe_cast(PROFISPRONT AS STRING) AS profis_prontuario,
    safe_cast(PROFISSIONAL AS STRING) AS profissional,
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
    safe_cast(JUSTIFICATIVA AS STRING) AS justificativa,
    _airbyte_extracted_at as datalake_loaded_at, 
current_timestamp() as datalake_transformed_at 
FROM {{ source('brutos_recursos_humanos_ergon_pericia_medica_staging', 'ERG_PM_PRONT_MED') }}

