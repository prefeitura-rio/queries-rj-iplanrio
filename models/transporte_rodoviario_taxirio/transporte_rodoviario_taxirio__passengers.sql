SELECT
  SAFE_CAST (ano_particao as INT64) as ano_particao,
  SAFE_CAST (mes_particao as INT64) as mes_particao,
  SAFE_CAST (dia_particao as INT64) as dia_particao,
  DATETIME (TIMESTAMP(createdAt)) as data_criacao,
  SAFE_CAST (id as STRING) as id_passageiro,
  SAFE_CAST (user as STRING) as id_usuario,
  SAFE_CAST (isAbleToUsePaymentInApp as BOOL) as pode_usar_pagamento_app,
  DATETIME (TIMESTAMP(infoPhone_updatedAt)) as phone_atualizacao,
  SAFE_CAST (infoPhone_id as STRING) as id_phone,
  SAFE_CAST (infoPhone_appVersion as STRING) as phone_versao_app,
  SAFE_CAST (infoPhone_phoneModel as STRING) as phone_modelo,
  SAFE_CAST (infoPhone_phoneManufacturer as STRING) as phone_fabricante,
  SAFE_CAST (infoPhone_osVersion as STRING) as phone_versao_sistema,
  SAFE_CAST (infoPhone_osName as STRING) as phone_nome_sistema,
FROM
  `rj-iplanrio.transporte_rodoviario_taxirio_staging.passengers`
