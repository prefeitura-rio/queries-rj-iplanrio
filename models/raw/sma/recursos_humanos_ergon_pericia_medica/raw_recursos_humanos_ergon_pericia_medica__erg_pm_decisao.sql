{{
    config(
        alias='erg_pm_decisao'
    )
}}

SELECT safe_cast(SIGLA AS STRING) AS sigla, 
    safe_cast(NEGADO AS STRING) AS negado, 
    safe_cast(ALTA_AT AS STRING) AS alta_at, 
    safe_cast(DECISAO AS STRING) AS decisao, 
    safe_cast(RETIFICA AS STRING) AS retifica, 
    safe_cast(DESCRICAO AS STRING) AS descricao, 
    safe_cast(NUM_MESES AS INT64) AS num_meses, 
    safe_cast(TITULO_01 AS STRING) AS titulo_01, 
    safe_cast(TITULO_02 AS STRING) AS titulo_02, 
    safe_cast(TITULO_03 AS STRING) AS titulo_03, 
    safe_cast(SIGLAEXAME AS STRING) AS sigla_exame, 
    safe_cast(EXAME_ENCAM AS STRING) AS exame_encam, 
    safe_cast(TEXTO_LAUDO AS STRING) AS texto_laudo, 
    safe_cast(TITULO_LAUDO AS STRING) AS titulo_laudo, 
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
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at 
FROM {{ source('brutos_recursos_humanos_ergon_pericia_medica_staging', 'ERG_PM_DECISAO') }}

