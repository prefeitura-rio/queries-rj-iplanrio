{{
  config(
    materialized='table',
    alias='prpa'
  )
}}

SELECT
    id_prpa AS id_prpa,
    coddocumento AS id_processo,
    nome_prpa AS nome_prpa,
    cpf_prpa AS cpf_prpa,
    email_prpa AS email_prpa,
    telefone_prpa AS telefone_prpa,
    tipo_registro_prpa AS tipo_registro_prpa,
    numero_registro_prpa AS numero_registro_prpa,
    tipo_conselho_prpa AS tipo_conselho_prpa,
    numero_conselho_prpa AS numero_conselho_prpa,
    CAST(CAST(id_profissao AS FLOAT64) AS INT64) AS id_profissao,

FROM {{ source('adm_licenca_urbanismo_staging', 'prpa') }}
