{{
    config(
        alias='unidade_organizacional',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(ds_bi AS STRING) AS ds_bi,
    SAFE_CAST(ds_email AS STRING) AS ds_email,
    SAFE_CAST(ds_endereco_numero AS INT64) AS ds_endereco_numero,
    SAFE_CAST(ds_sigla AS STRING) AS ds_sigla,
    SAFE_CAST(ds_telefone_1 AS INT64) AS ds_telefone_1,
    SAFE_CAST(ds_telefone_2 AS INT64) AS ds_telefone_2,
    SAFE_CAST(ds_telefone_3 AS INT64) AS ds_telefone_3,
    SAFE_CAST(ds_url_webservice AS STRING) AS ds_url_webservice,
    SAFE_CAST(fl_ativo AS BOOL) AS fl_ativo,
    SAFE_CAST(fl_bi_incluir AS BOOL) AS fl_bi_incluir,
    SAFE_CAST(fl_integrar_chamado AS BOOL) AS fl_integrar_chamado,
    SAFE_CAST(fl_orgao AS BOOL) AS fl_orgao,
    SAFE_CAST(fl_ouvidoria AS BOOL) AS fl_ouvidoria,
    SAFE_CAST(fl_recebe_email_multi_transferencia AS BOOL) AS fl_recebe_email_multi_transferencia,
    SAFE_CAST(id_bairro_logradouro_fk AS INT64) AS id_bairro_logradouro_fk,
    SAFE_CAST(id_instituicao_fk AS INT64) AS id_instituicao_fk,
    SAFE_CAST(id_unidade_organizacional AS INT64) AS id_unidade_organizacional,
    SAFE_CAST(id_unidade_pai_fk AS INT64) AS id_unidade_pai_fk,
    SAFE_CAST(no_unidade_organizacional AS STRING) AS no_unidade_organizacional,
    SAFE_CAST(nu_expediente_fim AS INT64) AS nu_expediente_fim,
    SAFE_CAST(nu_expediente_inicio AS INT64) AS nu_expediente_inicio,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_unidade_organizacional') }}
