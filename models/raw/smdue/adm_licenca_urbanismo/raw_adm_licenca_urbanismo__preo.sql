{{
  config(
    materialized='table',
    alias='preo'
  )
}}

SELECT
    id_preo AS id_preo,
    coddocumento AS id_documento,
    nome_preo AS nome_preo,
    cpf_preo AS cpf_preo,
    email_preo AS email_preo,
    telefone_preo AS telefone_preo,
    tipo_registro_preo AS tipo_registro_preo,
    numero_registro_preo AS numero_registro_preo,
    tipo_conselho_preo AS tipo_conselho_preo,
    numero_conselho_preo AS numero_conselho_preo,
    id_profissao AS id_profissao,

FROM {{ source('adm_licenca_urbanismo_staging', 'preo') }}
