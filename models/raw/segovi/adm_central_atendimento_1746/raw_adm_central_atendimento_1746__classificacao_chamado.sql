{{
    config(
        alias='classificacao_chamado',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(ds_anotacao AS STRING) AS ds_anotacao,
    SAFE_CAST(ds_despacho AS STRING) AS ds_despacho,
    SAFE_CAST(ds_diagnostico AS STRING) AS ds_diagnostico,
    SAFE_CAST(ds_diagnostico_moderado AS STRING) AS ds_diagnostico_moderado,
    SAFE_CAST(ds_diagnostico_traduzido AS STRING) AS ds_diagnostico_traduzido,
    SAFE_CAST(dt_classificacao AS DATETIME) AS dt_classificacao,
    SAFE_CAST(dt_envio AS DATETIME) AS dt_envio,
    SAFE_CAST(dt_info_sistema AS DATETIME) AS dt_info_sistema,
    SAFE_CAST(dt_prevista_atendimento AS DATETIME) AS dt_prevista_atendimento,
    SAFE_CAST(dt_real_diagnostico AS DATETIME) AS dt_real_diagnostico,
    SAFE_CAST(fl_aceito AS BOOL) AS fl_aceito,
    SAFE_CAST(fl_dados_pessoais AS BOOL) AS fl_dados_pessoais,
    SAFE_CAST(fl_enviado AS BOOL) AS fl_enviado,
    SAFE_CAST(fl_publico AS BOOL) AS fl_publico,
    SAFE_CAST(id_cenario_fk AS INT64) AS id_cenario_fk,
    SAFE_CAST(id_chamado_fk AS INT64) AS id_chamado_fk,
    SAFE_CAST(id_classificacao_chamado AS INT64) AS id_classificacao_chamado,
    SAFE_CAST(id_classificacao_fk AS INT64) AS id_classificacao_fk,
    SAFE_CAST(id_justificativa_fk AS INT64) AS id_justificativa_fk,
    SAFE_CAST(id_pessoa_diagnostico_fk AS INT64) AS id_pessoa_diagnostico_fk,
    SAFE_CAST(id_pessoa_fk AS INT64) AS id_pessoa_fk,
    SAFE_CAST(id_pessoa_moderador_fk AS INT64) AS id_pessoa_moderador_fk,
    SAFE_CAST(id_pessoa_programacao_fk AS INT64) AS id_pessoa_programacao_fk,
    SAFE_CAST(id_status_especifico_fk AS INT64) AS id_status_especifico_fk,
    SAFE_CAST(id_subtipo_fk AS INT64) AS id_subtipo_fk,
    SAFE_CAST(id_unidade_organizacional_fk AS INT64) AS id_unidade_organizacional_fk,
    SAFE_CAST(no_status_especifico AS STRING) AS no_status_especifico,
    SAFE_CAST(nu_tentativa_integracao AS INT64) AS nu_tentativa_integracao,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_classificacao_chamado') }}
