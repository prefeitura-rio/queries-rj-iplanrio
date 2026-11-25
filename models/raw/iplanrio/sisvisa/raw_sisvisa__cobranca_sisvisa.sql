{{ config(alias='cobranca_sisvisa', materialized='table') }}

SELECT
CAST(`CD_LINHA_DIGITAVEL` AS STRING) AS cd_linha_digitavel,
CAST(`CD_RECEITA` AS INT64) AS cd_receita,
CAST(`CD_TIPO_REQUERIMENTO` AS INT64) AS cd_tipo_requerimento,
CAST(`DT_AVISO_EXPIRACAO` AS DATETIME) AS dt_aviso_expiracao,
CAST(`DT_COMPETENCIA` AS DATETIME) AS dt_competencia,
CAST(`DT_PAGAMENTO` AS DATETIME) AS dt_pagamento,
CAST(`DT_VENCTO_1` AS DATETIME) AS dt_vencto_1,
CAST(`DT_VENCTO_2` AS DATETIME) AS dt_vencto_2,
CAST(`ID_REQUERIMENTO` AS INT64) AS id_requerimento,
CAST(`NM_TIPO_LICENCIAMENTO` AS STRING) AS nm_tipo_licenciamento,
CAST(`NR_CODIGO_BARRA` AS STRING) AS nr_codigo_barra,
CAST(`NR_CPF` AS STRING) AS nr_cpf,
CAST(`NR_GUIA` AS INT64) AS nr_guia,
CAST(`SITUACAO` AS INT64) AS situacao,
CAST(`TP_IMPOSTO` AS INT64) AS tp_imposto,
CAST(`TP_MEI` AS BOOL) AS tp_mei,
CAST(`TX_GUIA` AS STRING) AS tx_guia,
CAST(`TX_INFO_COMPLEMENTAR` AS STRING) AS tx_info_complementar,
CAST(`VL_MORA` AS FLOAT64) AS vl_mora,
CAST(`VL_MULTA` AS FLOAT64) AS vl_multa,
CAST(`VL_PRINCIPAL` AS FLOAT64) AS vl_principal,
CAST(`VL_PRINCIPALDESC` AS FLOAT64) AS vl_principaldesc,
CAST(`VL_RECEITA` AS FLOAT64) AS vl_receita,
CAST(_airbyte_extracted_at AS DATETIME) AS loaded_at
FROM {{ source('brutos_sisvisa_staging', 'CobrancaSisvisa') }}
