{{ config(alias='atividade_estabelecimento', materialized='table') }}

SELECT
CAST(`afe` AS STRING) AS afe,
CAST(`Codigo` AS STRING) AS codigo,
CAST(`Complemento` AS STRING) AS complemento,
CAST(`Descricao` AS STRING) AS descricao,
CAST(`EstabelecimentoId` AS INT64) AS estabelecimento_id,
CAST(`Licenciada` AS BOOL) AS licenciada,
CAST(`NecessitaComplemento` AS BOOL) AS necessita_complemento,
CAST(`Regulada` AS BOOL) AS regulada,
CAST(`TemInternacao` AS BOOL) AS tem_internacao,
CAST(`TemInvasivo` AS BOOL) AS tem_invasivo,
CAST(`TemProcedimentoInvasivo` AS BOOL) AS tem_procedimento_invasivo,
CAST(_airbyte_extracted_at AS DATETIME) AS loaded_at
FROM {{ source('brutos_sisvisa_staging', 'AtividadesDoEstabelecimento') }}
