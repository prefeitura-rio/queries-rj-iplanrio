{{
    config(
        alias='subtipo',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(ds_palavra_chave AS STRING) AS ds_palavra_chave,
    SAFE_CAST(ds_script_atendimento AS STRING) AS ds_script_atendimento,
    SAFE_CAST(fl_ativo AS BOOL) AS fl_ativo,
    SAFE_CAST(fl_nao_selecionavel_atendimento AS BOOL) AS fl_nao_selecionavel_atendimento,
    SAFE_CAST(ic_grau_prioridade AS STRING) AS ic_grau_prioridade,
    SAFE_CAST(id_subtipo AS INT64) AS id_subtipo,
    SAFE_CAST(id_tipo_fk AS INT64) AS id_tipo_fk,
    SAFE_CAST(no_subtipo AS STRING) AS no_subtipo,
    SAFE_CAST(no_subtipo_espanhol AS STRING) AS no_subtipo_espanhol,
    SAFE_CAST(no_subtipo_ingles AS STRING) AS no_subtipo_ingles,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_subtipo') }}
