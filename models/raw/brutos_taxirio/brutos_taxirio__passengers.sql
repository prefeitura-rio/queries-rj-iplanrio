

SELECT
  
  SAFE_CAST (id as STRING) as id_passageiro,
  SAFE_CAST (user as STRING) as id_usuario,
  DATETIME (TIMESTAMP(createdAt)) as data_criacao,
  SAFE_CAST (login as STRING) as usuario,
  SAFE_CAST (password as STRING) as senha,
  SAFE_CAST (salt as STRING) as aleatorio,
  SAFE_CAST (isAbleToUsePaymentInApp as BOOL) as pode_usar_pagamento_app,
  DATETIME (TIMESTAMP(infoPhone_updatedAt)) as data_atualizacao_telefone,
  SAFE_CAST (infoPhone_id as STRING) as id_telefone,
  SAFE_CAST (infoPhone_appVersion as STRING) as versao_app_telefone,
  SAFE_CAST (infoPhone_phoneModel as STRING) as modelo_telefone,
  SAFE_CAST (infoPhone_phoneManufacturer as STRING) as fabricante_telefone,
  SAFE_CAST (infoPhone_osVersion as STRING) as versao_sistema_telefone,
  SAFE_CAST (infoPhone_osName as STRING) as nome_sistema_telefone,
  SAFE_CAST (tokenInfo_httpSalt as STRING) as ficha_http_aleatorio,
  SAFE_CAST (tokenInfo_wssSalt as STRING) as ficha_wss_aleatorio,
  SAFE_CAST (tokenInfo_pushToken as STRING) as ficha_envio,
  SAFE_CAST (ano_particao as INT64) as ano_particao,
  SAFE_CAST (mes_particao as INT64) as mes_particao,
  
FROM
  {{ source('brutos_taxirio_staging','passengers') }}
