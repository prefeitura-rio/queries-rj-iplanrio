{{
  config(
    materialized='table',
    alias='tramite_processo'
  )
}}

SELECT
codtramite AS id_tramite,
matriccadastrador AS matricula_cadastrador,
coddocumento AS id_documento,
CAST(dtcadastro AS DATETIME) AS data_cadastro,
CAST(dtsaida AS DATETIME) AS data_saida,
CAST(vfdestinoorgao AS BOOL) AS orgao_destino,
coddestino AS id_orgao_destino,
matricorigem AS matricula_origem,
matricdestino AS matricula_destino,
codorigem AS id_origem,
status AS status,

FROM {{ source('adm_licenca_urbanismo_staging', 'tramite_processo') }}
