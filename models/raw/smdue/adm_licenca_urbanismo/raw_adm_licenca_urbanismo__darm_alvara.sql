{{
  config(
    materialized='table',
    alias='darm_alvara'
  )
}}

SELECT
num_darm AS id_darm,
darm AS numero_darm,
num_lic AS id_licenciamento,
cnpj_cpf AS cnpj_cpf,
cod_receita AS codigo_receita,
CAST(dt_emissao AS DATETIME) AS data_emissao,
CAST(dt_venc AS DATETIME) AS data_vencimento,
CAST(dt_pagamento AS DATETIME) AS data_pagamento,
CAST(CAST(competencia AS FLOAT64) AS int64) AS competencia,
CAST(mora AS FLOAT64) AS mora,
CAST(multa AS FLOAT64) AS multa,
CAST(valor_darm_inicial AS FLOAT64) AS valor_darm_inicial,
CAST(valordarm AS FLOAT64) AS valor_darm,
CAST(cancelado AS BOOL) AS cancelado,
CAST(dt_calculo AS DATETIME) AS data_calculo,
CAST(insciptu AS STRING) AS numero_inscricao_imovel_iptu,
CAST(avulso AS BOOL) AS darm_avulso,

FROM {{ source('adm_licenca_urbanismo_staging', 'darm_alvara') }}
