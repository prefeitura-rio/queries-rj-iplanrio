{{
  config(
    materialized='table',
    alias='documento'
  )
}}

SELECT
    codDocumento AS id_documento,
    numDocumento AS num_documento,
    codOrgao AS cod_orgao,
    codTipoDocumento AS cod_tipo_documento,
    MatricCadastrador AS matricula_cadastrador,
    CAST(dtAbertura AS DATETIME) AS dt_abertura,
    CAST(dtCadastroSistema AS DATETIME) AS dt_cadastro_sistema,
    codAssunto AS id_assunto,
    codDocumentoOrigem AS id_documento_origem,
    descStatus AS status_documento,
    id_classificacao_processo AS id_classificacao_processo,
    id_origem_classificacao_processo AS id_origem_classificacao_processo,
    id_tipo_processo AS id_tipo_processo,
    CAST(CAST(ano_requerimento AS FLOAT64) AS INT64) AS ano_requerimento,
    CAST(CAST(numero_requerimento AS FLOAT64) AS INT64) AS numero_requerimento,

FROM {{ source('adm_licenca_urbanismo_staging', 'documento') }}
