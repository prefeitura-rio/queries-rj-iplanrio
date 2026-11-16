{{
    config(
        alias='responsavel_chamado',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(cd_ocorrencia AS STRING) AS cd_ocorrencia,
    SAFE_CAST(dt_aceito AS DATETIME) AS dt_aceito,
    SAFE_CAST(dt_aquisicao AS DATETIME) AS dt_aquisicao,
    SAFE_CAST(dt_enviado AS DATETIME) AS dt_enviado,
    SAFE_CAST(dt_incompleto_retido AS DATETIME) AS dt_incompleto_retido,
    SAFE_CAST(fl_aceito AS BOOL) AS fl_aceito,
    SAFE_CAST(fl_aguardando_integracao AS BOOL) AS fl_aguardando_integracao,
    SAFE_CAST(fl_contingencia AS BOOL) AS fl_contingencia,
    SAFE_CAST(fl_enviado AS BOOL) AS fl_enviado,
    SAFE_CAST(fl_recebido AS BOOL) AS fl_recebido,
    SAFE_CAST(fl_reintegracao_manual AS BOOL) AS fl_reintegracao_manual,
    SAFE_CAST(id_chamado_fk AS INT64) AS id_chamado_fk,
    SAFE_CAST(id_pessoa_fk AS INT64) AS id_pessoa_fk,
    SAFE_CAST(id_responsavel_chamado AS INT64) AS id_responsavel_chamado,
    SAFE_CAST(id_unidade_organizacional_fk AS INT64) AS id_unidade_organizacional_fk,
    SAFE_CAST(nu_tentativa_integracao AS INT64) AS nu_tentativa_integracao,
    SAFE_CAST(nu_tentativa_integracao_incompleto AS INT64) AS nu_tentativa_integracao_incompleto,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_responsavel_chamado') }}
