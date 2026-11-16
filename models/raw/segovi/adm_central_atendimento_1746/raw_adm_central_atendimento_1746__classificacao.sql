{{
    config(
        alias='classificacao',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(fl_envia_sms_fechamento AS BOOL) AS fl_envia_sms_fechamento,
    SAFE_CAST(fl_exibe_alerta_monitoramento AS BOOL) AS fl_exibe_alerta_monitoramento,
    SAFE_CAST(fl_exibe_arvore_chamado AS BOOL) AS fl_exibe_arvore_chamado,
    SAFE_CAST(fl_exige_solicitante AS BOOL) AS fl_exige_solicitante,
    SAFE_CAST(fl_integrada_portal_web AS BOOL) AS fl_integrada_portal_web,
    SAFE_CAST(fl_nao_exibir_script_atendimento AS BOOL) AS fl_nao_exibir_script_atendimento,
    SAFE_CAST(fl_prazo_corrido AS BOOL) AS fl_prazo_corrido,
    SAFE_CAST(fl_resolver AS BOOL) AS fl_resolver,
    SAFE_CAST(fl_sigilo_obrigatorio AS BOOL) AS fl_sigilo_obrigatorio,
    SAFE_CAST(ic_prazo_unidade_tempo AS STRING) AS ic_prazo_unidade_tempo,
    SAFE_CAST(id_categoria_fk AS INT64) AS id_categoria_fk,
    SAFE_CAST(id_classificacao AS INT64) AS id_classificacao,
    SAFE_CAST(id_subtipo_fk AS INT64) AS id_subtipo_fk,
    SAFE_CAST(id_unidade_organizacional_fk AS INT64) AS id_unidade_organizacional_fk,
    SAFE_CAST(nu_prazo AS INT64) AS nu_prazo,
    SAFE_CAST(nu_prazo_reincidencia AS INT64) AS nu_prazo_reincidencia,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_classificacao') }}
