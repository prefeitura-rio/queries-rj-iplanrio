{{
    config(
        alias='status_especifico',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(fl_ativo AS BOOL) AS fl_ativo,
    SAFE_CAST(fl_encerramento_atendimento AS BOOL) AS fl_encerramento_atendimento,
    SAFE_CAST(fl_moderacao_obrigatoria AS BOOL) AS fl_moderacao_obrigatoria,
    SAFE_CAST(fl_publico AS BOOL) AS fl_publico,
    SAFE_CAST(id_status_especifico AS INT64) AS id_status_especifico,
    SAFE_CAST(id_status_fk AS INT64) AS id_status_fk,
    SAFE_CAST(id_unidade_organizacional_fk AS INT64) AS id_unidade_organizacional_fk,
    SAFE_CAST(no_status_especifico AS STRING) AS no_status_especifico,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_status_especifico') }}
