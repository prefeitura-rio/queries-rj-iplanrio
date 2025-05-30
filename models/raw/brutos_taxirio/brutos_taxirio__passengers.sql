
{{
  config(
    schema= 'brutos_taxirio',
    alias= 'passageiros',
    materialized='table',
    partition_by={
      'field': 'data_particao',
      'data_type': 'date'
    },


)}}


SELECT
  
  SAFE_CAST (id as STRING) as id_passageiro,
  SAFE_CAST (user as STRING) as id_usuario,
  DATETIME (TIMESTAMP(createdAt)) as data_criacao,
  SAFE_CAST (login as STRING) as usuario,
  SAFE_CAST (password as STRING) as senha,
  SAFE_CAST (salt as STRING) as aleatorio,
  SAFE_CAST (isAbleToUsePaymentInApp as BOOL) as pode_usar_pagamento_app,
  DATETIME (TIMESTAMP(infoPhone_updatedAt)) as phone_atualizacao,
  SAFE_CAST (infoPhone_id as STRING) as id_phone,
  SAFE_CAST (infoPhone_appVersion as STRING) as phone_versao_app,
  SAFE_CAST (infoPhone_phoneModel as STRING) as phone_modelo,
  SAFE_CAST (infoPhone_phoneManufacturer as STRING) as phone_fabricante,
  SAFE_CAST (infoPhone_osVersion as STRING) as phone_versao_sistema,
  SAFE_CAST (infoPhone_osName as STRING) as phone_nome_sistema,
  SAFE_CAST (tokenInfo_httpSalt as STRING) as ficha_http_aleatorio,
  SAFE_CAST (tokenInfo_wssSalt as STRING) as ficha_wss_aleatorio,
  SAFE_CAST (tokenInfo_pushToken as STRING) as ficha_envio,
  SAFE_CAST (ano_particao as INT64) as ano_particao,
  SAFE_CAST (mes_particao as INT64) as mes_particao,
  SAFE_CAST (dia_particao as INT64) as dia_particao,
  DATE(SAFE_CAST(ano_particao AS INT64), SAFE_CAST(mes_particao AS INT64), 1) AS data_particao
FROM
  `rj-iplanrio.brutos_taxirio_staging.passengers`
