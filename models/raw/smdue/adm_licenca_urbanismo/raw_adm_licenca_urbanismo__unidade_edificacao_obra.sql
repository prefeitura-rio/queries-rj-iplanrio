{{
  config(
    materialized='table',
    alias='unidade_edificacao_obra'
  )
}}

SELECT
id_unid AS id_unidade,
num_lic AS id_licenciamento,
id_edif AS id_edificacao,
id_subdivisao AS id_subdivisao,
cod_unidade AS id_tipo_unidade,
compltipo_unid AS descricao_tipo_unidade,
CAST(CAST(quant_unid AS FLOAT64) AS INT64) AS quantidade_unidades,
num_recebida AS identificacao_unidade,
CAST(area_unid AS FLOAT64) AS area_unidades

FROM {{ source('adm_licenca_urbanismo_staging', 'unidade_edificacao_obra') }}
