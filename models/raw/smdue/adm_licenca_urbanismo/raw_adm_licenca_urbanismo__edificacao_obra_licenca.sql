{{
  config(
    materialized='table',
    alias='edificacao_obra_licenca'
  )
}}

SELECT
id_edif AS id_edificacao,
num_edif AS identificacao_edificacao,
num_lic AS id_licenciamento,
cod_tp_edif AS id_tipo_edificacao,
cod_compl_tp_edif AS id_complemento_tipo_edificacao,
cod_compl_tp_edif2 AS id_complemento_tipo_edificacao_2,
compl_livre AS complemento,
CAST(area_edif AS FLOAT64) AS area_edificacao,
CAST(area_acresc AS FLOAT64) AS area_acrescida,
CAST(area_reduzida AS FLOAT64) AS area_reduzida,
CAST(area_util AS FLOAT64) AS area_util,
CAST(embasamento AS BOOL) AS embasamento,
CAST(recebenumeracao AS BOOL) AS recebe_numeracao,
CAST(contaedificacao AS BOOL) AS conta_edificacao,
CAST(CAST(qtd_edif AS FLOAT64) AS int64) AS total_edificacoes,

FROM {{ source('adm_licenca_urbanismo_staging', 'edificacao_obra_licenca') }}
