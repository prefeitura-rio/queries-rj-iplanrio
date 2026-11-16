{{
    config(
        alias='protocolo_chamado',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(dt_insercao AS DATETIME) AS dt_insercao,
    SAFE_CAST(fl_ativo AS BOOL) AS fl_ativo,
    SAFE_CAST(fl_enviado AS BOOL) AS fl_enviado,
    SAFE_CAST(ic_motivo AS STRING) AS ic_motivo,
    SAFE_CAST(id_chamado_fk AS INT64) AS id_chamado_fk,
    SAFE_CAST(id_pessoa_operador_fk AS INT64) AS id_pessoa_operador_fk,
    SAFE_CAST(id_protocolo_chamado AS INT64) AS id_protocolo_chamado,
    SAFE_CAST(id_protocolo_fk AS INT64) AS id_protocolo_fk,
    SAFE_CAST(nu_tentativa_integracao AS INT64) AS nu_tentativa_integracao,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_protocolo_chamado') }}
