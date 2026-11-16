{{
    config(
        alias='chamado_vinculado',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(dt_insercao AS DATETIME) AS dt_insercao,
    SAFE_CAST(fl_aceito AS BOOL) AS fl_aceito,
    SAFE_CAST(fl_enviado AS BOOL) AS fl_enviado,
    SAFE_CAST(ic_vinculo AS STRING) AS ic_vinculo,
    SAFE_CAST(id_chamado_filho_fk AS INT64) AS id_chamado_filho_fk,
    SAFE_CAST(id_chamado_pai_fk AS INT64) AS id_chamado_pai_fk,
    SAFE_CAST(id_chamado_vinculado AS INT64) AS id_chamado_vinculado,
    SAFE_CAST(id_status_fk AS INT64) AS id_status_fk,
    SAFE_CAST(nu_reiteracoes AS INT64) AS nu_reiteracoes,
    SAFE_CAST(nu_tentativa_integracao AS INT64) AS nu_tentativa_integracao,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_chamado_vinculado') }}
