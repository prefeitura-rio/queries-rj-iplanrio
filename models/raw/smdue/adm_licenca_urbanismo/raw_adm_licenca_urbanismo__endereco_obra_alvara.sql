{{
  config(
    materialized='table',
    alias='endereco_obra_alvara'
  )
}}

SELECT
id_endereco AS id_endereco,
num_lic AS id_licenciamento,
clogra AS id_logradouro,
tipo AS tipo_logradouro,
nobreza AS nobreza_logradouro,
preposicao AS preposicao_logradouro,
nomelogra AS nome_logradouro,
num AS numero_porta,

FROM {{ source('adm_licenca_urbanismo_staging', 'endereco_obra_alvara') }}
