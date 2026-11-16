{{
    config(
        alias='andamento',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(dt_insercao AS DATETIME) AS dt_insercao,
    SAFE_CAST(fl_aceito AS BOOL) AS fl_aceito,
    SAFE_CAST(fl_dados_pessoais AS BOOL) AS fl_dados_pessoais,
    SAFE_CAST(fl_enviado AS BOOL) AS fl_enviado,
    SAFE_CAST(fl_providencia_siso AS BOOL) AS fl_providencia_siso,
    SAFE_CAST(fl_publico AS BOOL) AS fl_publico,
    SAFE_CAST(id_andamento AS INT64) AS id_andamento,
    SAFE_CAST(id_andamento_pai_fk AS INT64) AS id_andamento_pai_fk,
    SAFE_CAST(id_chamado_fk AS INT64) AS id_chamado_fk,
    SAFE_CAST(id_justificativa_fk AS INT64) AS id_justificativa_fk,
    SAFE_CAST(id_pessoa_fk AS INT64) AS id_pessoa_fk,
    SAFE_CAST(id_pessoa_moderador_fk AS INT64) AS id_pessoa_moderador_fk,
    SAFE_CAST(id_status_especifico_fk AS INT64) AS id_status_especifico_fk,
    SAFE_CAST(id_unidade_organizacional_fk AS INT64) AS id_unidade_organizacional_fk,
    SAFE_CAST(no_status_unidade AS STRING) AS no_status_unidade,
    SAFE_CAST(nu_tentativa_integracao AS INT64) AS nu_tentativa_integracao,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_andamento') }}
