{{
  config(
    materialized='table',
    alias='linha_publicacao_tramite'
  )
}}

SELECT
codlinha AS id_linha,
codtramite AS id_tramite,
codpublicacao AS id_publicacao,
codorgao AS id_orgao,
desctipodespacho AS descricao_tipo_despacho,
numero AS numero_documento,
texto AS texto,
texto_final AS texto_final,
requerente AS nome_requerente,
matricliberador AS matricula_liberador,
CAST(dtcadastro AS DATETIME) AS data_cadastro,
CAST(dtexpediente AS DATETIME) AS data_expediente,
CAST(dtliberacao AS DATETIME) AS data_liberacao,

FROM {{ source('adm_licenca_urbanismo_staging', 'linha_publicacao_tramite') }}
