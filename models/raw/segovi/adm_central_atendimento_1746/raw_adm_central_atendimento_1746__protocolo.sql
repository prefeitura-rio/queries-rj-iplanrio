{{
    config(
        alias='protocolo',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(ds_codigo_fk AS STRING) AS ds_codigo_fk,
    SAFE_CAST(dt_atualizacao AS DATETIME) AS dt_atualizacao,
    SAFE_CAST(dt_fim AS DATETIME) AS dt_fim,
    SAFE_CAST(dt_inicio AS DATETIME) AS dt_inicio,
    SAFE_CAST(fl_solicitante_sigiloso AS BOOL) AS fl_solicitante_sigiloso,
    SAFE_CAST(id_pessoa_fk AS INT64) AS id_pessoa_fk,
    SAFE_CAST(id_protocolo AS INT64) AS id_protocolo,
    SAFE_CAST(id_submotivo_fk AS INT64) AS id_submotivo_fk,
FROM {{ source('brutos_1746_staging_airbyte', 'vw_protocolo') }}
