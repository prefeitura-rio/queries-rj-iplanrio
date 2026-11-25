{{ config(alias='historico_licenca', materialized='table') }}

SELECT
CAST(`AmbulanteId` AS INT64) AS ambulante_id,
CAST(`Area` AS INT64) AS area,
CAST(`BancaDeJornalId` AS INT64) AS banca_de_jornal_id,
CAST(`Complexidade` AS INT64) AS complexidade,
CAST(`DataAnulacao` AS DATETIME) AS data_anulacao,
CAST(`DataCassacao` AS DATETIME) AS data_cassacao,
CAST(`DataDeCancelamento` AS DATETIME) AS data_de_cancelamento,
CAST(`DataDeCriacao` AS DATETIME) AS data_de_criacao,
CAST(`DataDeRevogacao` AS DATETIME) AS data_de_revogacao,
CAST(`DataDeValidade` AS DATETIME) AS data_de_validade,
CAST(`DataReativacao` AS DATETIME) AS data_reativacao,
CAST(`EstabelecimentoId` AS INT64) AS estabelecimento_id,
CAST(`FeiranteId` AS INT64) AS feirante_id,
CAST(`Id` AS INT64) AS id,
CAST(`JustificativaId` AS INT64) AS justificativa_id,
CAST(`Licenciamento` AS INT64) AS licenciamento,
CAST(`Motivo` AS STRING) AS motivo,
CAST(`NumeroDaLicenca` AS STRING) AS numero_da_licenca,
CAST(`Risco` AS INT64) AS risco,
CAST(`SituacaoDaLicenca` AS INT64) AS situacao_da_licenca,
CAST(_airbyte_extracted_at AS DATETIME) AS loaded_at
FROM {{ source('brutos_sisvisa_staging', 'HistoricoDeLicenca') }}
