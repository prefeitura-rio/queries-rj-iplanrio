{{
  config(
    materialized='table',
    alias='tipo_parecer_tramite'
  )
}}

SELECT
codparecer AS id_parecer,
descparecer AS descricao_parecer,
CAST(CAST(vfgerapublicacao AS FLOAT64) AS INT64) AS gera_publicacao,
descclassificacao AS classificacao,
CAST(publicacao_frente_do AS BOOL) AS publicacao_frente_do,

FROM {{ source('adm_licenca_urbanismo_staging', 'tipo_parecer_tramite') }}
