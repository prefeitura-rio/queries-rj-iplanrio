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
    {{ clean_and_cast('id_pessoa', 'int64') }} id_pessoa,
    {{ clean_and_cast('ds_cpf', 'string') }} cpf,
    {{ clean_and_cast('no_pessoa', 'string') }} nome,
    {{ clean_and_cast('ds_identidade', 'string') }} documento_identidade,
    {{ clean_and_cast('nu_matricula', 'string') }} numero_matricula,
    {{ clean_and_cast('no_mae', 'string') }} nome_mae,
    SAFE_CAST(
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d', dt_nascimento) AS DATE -- TODO: review date format
    ) data_nascimento,
    {{ clean_and_cast('ic_sexo', 'string') }} sexo,
    {{ clean_and_cast('ds_atividade_profissional', 'string') }} atividade_profissional,

    -- Chaves Estrangeiras (FKs)
    {{ clean_and_cast('id_cargo_fk', 'int64') }} id_cargo,
    {{ clean_and_cast('id_idioma_fk', 'int64') }} id_idioma,
    {{ clean_and_cast('id_pais_ddi_fk', 'int64') }} id_pais_ddi,
    {{ clean_and_cast('id_tratamento_fk', 'int64') }} id_tratamento,
    {{ clean_and_cast('id_escolaridade_fk', 'int64') }} id_escolaridade,
    {{ clean_and_cast('id_tipo_endereco_fk', 'int64') }} id_tipo_endereco,
    {{ clean_and_cast('id_bairro_logradouro_fk', 'int64') }} id_bairro_logradouro,
    {{ clean_and_cast('id_unidade_organizacional_fk', 'int64') }} id_unidade_organizacional,

    -- Contato (Email e Telefone)
    {{ clean_and_cast('ds_email', 'string') }} email,
    {{ clean_and_cast('ds_telefone_1', 'string') }} telefone_1,
    {{ clean_and_cast('ds_telefone_2', 'string') }} telefone_2,
    {{ clean_and_cast('ds_telefone_3', 'string') }} telefone_3,

    -- Endereço
    {{ clean_and_cast('ds_endereco', 'string') }} endereco,
    SAFE_CAST(
        SAFE_CAST(ds_endereco_numero AS FLOAT64) AS INT64
    ) endereco_numero,
    {{ clean_and_cast('ds_endereco_cep', 'string') }} endereco_cep,
    {{ clean_and_cast('ds_endereco_complemento', 'string') }} endereco_complemento,
    {{ clean_and_cast('ds_endereco_referencia', 'string') }} endereco_referencia,
    SAFE_CAST(
        SAFE_CAST(nu_coord_x AS FLOAT64) AS FLOAT64
    ) coordenada_x,
    SAFE_CAST(
        SAFE_CAST(nu_coord_y AS FLOAT64) AS FLOAT64
    ) coordenada_y,

    -- Flags/Status
    {{ clean_and_cast('fl_ativo', 'string') }} flag_ativo,
    {{ clean_and_cast('fl_auditor', 'string') }} flag_auditor,
    {{ clean_and_cast('fl_sms', 'string') }} flag_sms,
    {{ clean_and_cast('fl_email', 'string') }} flag_email,
    {{ clean_and_cast('fl_recebe_sms', 'string') }} flag_recebe_sms,
    {{ clean_and_cast('fl_recebe_email', 'string') }} flag_recebe_email,
    {{ clean_and_cast('fl_reintegracao_sms', 'string') }} flag_reintegracao_sms,
    {{ clean_and_cast('fl_reintegracao_email', 'string') }} flag_reintegracao_email,
    {{ clean_and_cast('fl_nao_reside_no_municipio', 'string') }} flag_nao_reside_no_municipio,
    {{ clean_and_cast('fl_recebe_notificacao_generica', 'string') }} flag_recebe_notificacao_generica,
    {{ clean_and_cast('fl_recebe_notificacao_alerta_rio', 'string') }} flag_recebe_notificacao_alerta_rio,
    {{ clean_and_cast('fl_recebe_notificacao_andamento_chamado', 'string') }} flag_recebe_notificacao_andamento_chamado,

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