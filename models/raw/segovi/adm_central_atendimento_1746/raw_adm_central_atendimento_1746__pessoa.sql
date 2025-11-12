{{
    config(
        alias='pessoa',
        schema='adm_central_atendimento_1746',
        materialized='table',
        unique_key='id_pessoa',
    )
}}


WITH source_data AS (
  SELECT 
    _airbyte_extracted_at,
    CAST(ds_cpf AS STRING) AS ds_cpf,
    CAST(fl_sms AS STRING) AS fl_sms,
    CAST(no_mae AS STRING) AS no_mae,
    CAST(ic_sexo AS STRING) AS ic_sexo,
    CAST(ds_email AS STRING) AS ds_email,
    CAST(fl_ativo AS STRING) AS fl_ativo,
    CAST(fl_email AS STRING) AS fl_email,
    CAST(id_pessoa AS STRING) AS id_pessoa,
    CAST(no_pessoa AS STRING) AS no_pessoa,
    CAST(fl_auditor AS STRING) AS fl_auditor,
    CAST(nu_coord_x AS STRING) AS nu_coord_x,
    CAST(nu_coord_y AS STRING) AS nu_coord_y,
    CAST(ds_endereco AS STRING) AS ds_endereco,
    CAST(dt_insercao AS STRING) AS dt_insercao,
    CAST(id_cargo_fk AS STRING) AS id_cargo_fk,
    CAST(id_idioma_fk AS STRING) AS id_idioma_fk,
    CAST(nu_matricula AS STRING) AS nu_matricula,
    CAST(ds_identidade AS STRING) AS ds_identidade,
    CAST(ds_telefone_1 AS STRING) AS ds_telefone_1,
    CAST(ds_telefone_2 AS STRING) AS ds_telefone_2,
    CAST(ds_telefone_3 AS STRING) AS ds_telefone_3,
    CAST(dt_nascimento AS STRING) AS dt_nascimento,
    CAST(fl_recebe_sms AS STRING) AS fl_recebe_sms,
    CAST(dt_atualizacao AS STRING) AS dt_atualizacao,
    CAST(id_pais_ddi_fk AS STRING) AS id_pais_ddi_fk,
    CAST(ds_endereco_cep AS STRING) AS ds_endereco_cep,
    CAST(fl_recebe_email AS STRING) AS fl_recebe_email,
    CAST(fl_alterou_senha AS STRING) AS fl_alterou_senha,
    CAST(id_tratamento_fk AS STRING) AS id_tratamento_fk,
    CAST(ds_endereco_numero AS STRING) AS ds_endereco_numero,
    CAST(id_escolaridade_fk AS STRING) AS id_escolaridade_fk,
    CAST(fl_reintegracao_sms AS STRING) AS fl_reintegracao_sms,
    CAST(id_tipo_endereco_fk AS STRING) AS id_tipo_endereco_fk,
    CAST(fl_reintegracao_email AS STRING) AS fl_reintegracao_email,
    CAST(ds_endereco_referencia AS STRING) AS ds_endereco_referencia,
    CAST(ds_endereco_complemento AS STRING) AS ds_endereco_complemento,
    CAST(id_bairro_logradouro_fk AS STRING) AS id_bairro_logradouro_fk,
    CAST(ds_atividade_profissional AS STRING) AS ds_atividade_profissional,
    CAST(fl_nao_reside_no_municipio AS STRING) AS fl_nao_reside_no_municipio,
    CAST(id_unidade_organizacional_fk AS STRING) AS id_unidade_organizacional_fk,
    CAST(fl_recebe_notificacao_generica AS STRING) AS fl_recebe_notificacao_generica,
    CAST(fl_recebe_notificacao_alerta_rio AS STRING) AS fl_recebe_notificacao_alerta_rio,
    CAST(fl_recebe_notificacao_andamento_chamado AS STRING) AS fl_recebe_notificacao_andamento_chamado
  FROM {{ source('brutos_1746_staging_airbyte', 'tb_pessoa') }}
)

SELECT
   -- Chaves e Identificação Primária
    SAFE_CAST(
        REGEXP_REPLACE(id_pessoa, r'\.0$', '') AS INT64
    ) id_pessoa,
    SAFE_CAST(
        REGEXP_REPLACE(ds_cpf, r'\.0$', '') AS STRING
    ) cpf,
    SAFE_CAST(
        REGEXP_REPLACE(no_pessoa, r'\.0$', '') AS STRING
    ) nome,
    SAFE_CAST(
        REGEXP_REPLACE(ds_identidade, r'\.0$', '') AS STRING
    ) documento_identidade,
    SAFE_CAST(
        REGEXP_REPLACE(nu_matricula, r'\.0$', '') AS STRING
    ) numero_matricula,
    SAFE_CAST(
        REGEXP_REPLACE(no_mae, r'\.0$', '') AS STRING
    ) nome_mae,
    SAFE_CAST(
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d', dt_nascimento) AS DATE -- TODO: review date format
    ) data_nascimento,
    SAFE_CAST(
        REGEXP_REPLACE(ic_sexo, r'\.0$', '') AS STRING
    ) sexo,
    SAFE_CAST(
        REGEXP_REPLACE(ds_atividade_profissional, r'\.0$', '') AS STRING
    ) atividade_profissional,

    -- Chaves Estrangeiras (FKs)
    SAFE_CAST(
        REGEXP_REPLACE(id_cargo_fk, r'\.0$', '') AS INT64
    ) id_cargo,
    SAFE_CAST(
        REGEXP_REPLACE(id_idioma_fk, r'\.0$', '') AS INT64
    ) id_idioma,
    SAFE_CAST(
        REGEXP_REPLACE(id_pais_ddi_fk, r'\.0$', '') AS INT64
    ) id_pais_ddi,
    SAFE_CAST(
        REGEXP_REPLACE(id_tratamento_fk, r'\.0$', '') AS INT64
    ) id_tratamento,
    SAFE_CAST(
        REGEXP_REPLACE(id_escolaridade_fk, r'\.0$', '') AS INT64
    ) id_escolaridade,
    SAFE_CAST(
        REGEXP_REPLACE(id_tipo_endereco_fk, r'\.0$', '') AS INT64
    ) id_tipo_endereco,
    SAFE_CAST(
        REGEXP_REPLACE(id_bairro_logradouro_fk, r'\.0$', '') AS INT64
    ) id_bairro_logradouro,
    SAFE_CAST(
        REGEXP_REPLACE(id_unidade_organizacional_fk, r'\.0$', '') AS INT64
    ) id_unidade_organizacional,

    -- Contato (Email e Telefone)
    SAFE_CAST(
        REGEXP_REPLACE(ds_email, r'\.0$', '') AS STRING
    ) email,
    SAFE_CAST(
        REGEXP_REPLACE(ds_telefone_1, r'\.0$', '') AS STRING
    ) telefone_1,
    SAFE_CAST(
        REGEXP_REPLACE(ds_telefone_2, r'\.0$', '') AS STRING
    ) telefone_2,
    SAFE_CAST(
        REGEXP_REPLACE(ds_telefone_3, r'\.0$', '') AS STRING
    ) telefone_3,

    -- Endereço
    SAFE_CAST(
        REGEXP_REPLACE(ds_endereco, r'\.0$', '') AS STRING
    ) endereco,
    SAFE_CAST(
        SAFE_CAST(ds_endereco_numero AS FLOAT64) AS INT64
    ) endereco_numero,
    SAFE_CAST(
        REGEXP_REPLACE(ds_endereco_cep, r'\.0$', '') AS STRING
    ) endereco_cep,
    SAFE_CAST(
        REGEXP_REPLACE(ds_endereco_complemento, r'\.0$', '') AS STRING
    ) endereco_complemento,
    SAFE_CAST(
        REGEXP_REPLACE(ds_endereco_referencia, r'\.0$', '') AS STRING
    ) endereco_referencia,
    SAFE_CAST(
        SAFE_CAST(nu_coord_x AS FLOAT64) AS FLOAT64
    ) coordenada_x,
    SAFE_CAST(
        SAFE_CAST(nu_coord_y AS FLOAT64) AS FLOAT64
    ) coordenada_y,

    -- Flags/Status
    SAFE_CAST(
        REGEXP_REPLACE(fl_ativo, r'\.0$', '') AS STRING
    ) flag_ativo,
    SAFE_CAST(
        REGEXP_REPLACE(fl_auditor, r'\.0$', '') AS STRING
    ) flag_auditor,
    SAFE_CAST(
        REGEXP_REPLACE(fl_sms, r'\.0$', '') AS STRING
    ) flag_sms,
    SAFE_CAST(
        REGEXP_REPLACE(fl_email, r'\.0$', '') AS STRING
    ) flag_email,
    SAFE_CAST(
        REGEXP_REPLACE(fl_recebe_sms, r'\.0$', '') AS STRING
    ) flag_recebe_sms,
    SAFE_CAST(
        REGEXP_REPLACE(fl_recebe_email, r'\.0$', '') AS STRING
    ) flag_recebe_email,
    SAFE_CAST(
        REGEXP_REPLACE(fl_alterou_senha, r'\.0$', '') AS STRING
    ) flag_alterou_senha,
    SAFE_CAST(
        REGEXP_REPLACE(fl_reintegracao_sms, r'\.0$', '') AS STRING
    ) flag_reintegracao_sms,
    SAFE_CAST(
        REGEXP_REPLACE(fl_reintegracao_email, r'\.0$', '') AS STRING
    ) flag_reintegracao_email,
    SAFE_CAST(
        REGEXP_REPLACE(fl_nao_reside_no_municipio, r'\.0$', '') AS STRING
    ) flag_nao_reside_no_municipio,
    SAFE_CAST(
        REGEXP_REPLACE(fl_recebe_notificacao_generica, r'\.0$', '') AS STRING
    ) flag_recebe_notificacao_generica,
    SAFE_CAST(
        REGEXP_REPLACE(fl_recebe_notificacao_alerta_rio, r'\.0$', '') AS STRING
    ) flag_recebe_notificacao_alerta_rio,
    SAFE_CAST(
        REGEXP_REPLACE(fl_recebe_notificacao_andamento_chamado, r'\.0$', '') AS STRING
    ) flag_recebe_notificacao_andamento_chamado,

    -- Datas de Controle
    SAFE_CAST(
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', dt_insercao) AS DATETIME -- TODO: review timestamp format
    ) data_insercao,
    SAFE_CAST(
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', dt_atualizacao) AS DATETIME -- TODO: review timestamp format
    ) data_atualizacao,
    
    -- Metadados Airbyte
    _airbyte_extracted_at AS  extracted_at

FROM source_data AS t