{{
  config(
    schema='brutos_taxirio',
    alias='motoristas',
    materialized='table',
    partition_by={
      'field': 'data_particao',
      'data_type': 'date',
    }
  )
}}

SELECT
  SAFE_CAST (id as STRING) as id_motorista,
  SAFE_CAST (dia_particao as INT64) as dia_particao,
  DATETIME (TIMESTAMP(createdAt)) as data_criacao,
  SAFE_CAST (user as STRING) as id_usuario,
  SAFE_CAST (taxiDriverId as STRING) as id_motorista_taxi,
  SAFE_CAST (cars as STRING) as id_carro_taxi,
  SAFE_CAST (average as FLOAT64) as nota_media,
  SAFE_CAST (associatedCar as STRING) as id_carro_associado,
  SAFE_CAST (status as STRING) as status,
  SAFE_CAST (associatedDiscount as STRING) as id_desconto_associado,
  PARSE_JSON (associatedPaymentsMethods) as pagamento_metodo,
  SAFE_CAST (login as STRING) as usuario,
  SAFE_CAST (password as STRING) as senha,
  SAFE_CAST (salt as STRING) as dado_aleatorio,
  SAFE_CAST (isAbleToReceivePaymentInApp as bool) as pode_receber_pagamento_app,
  SAFE_CAST (isAbleToReceivePaymentInCityHall as bool) as pode_receber_pagamento_prefeitura,
  SAFE_CAST (ratingsReceived as INT64) as avaliacoes_recebidas,
  SAFE_CAST (busy as BOOL) as ocupado,
  SAFE_CAST (lastAverage as FLOAT64) as ultima_nota_media,
  DATETIME (TIMESTAMP(expiredBlockByRankingDate)) as data_termino_classificacao_bloqueio,
  SAFE_CAST (blockedRace as STRING) as id_corrida_bloqueada,
  SAFE_CAST (city as STRING) as id_municipio,
  SAFE_CAST (serviceRecordRate as FLOAT64) as taxa_servico,
  SAFE_CAST (nota as FLOAT64) as nota,
  SAFE_CAST (averageTT as FLOAT64) as mediatt,
  DATETIME (TIMESTAMP(infoPhone_updatedAt)) as phone_atualizacao,
  SAFE_CAST (infoPhone_id as STRING) as id_phone,
  SAFE_CAST (infoPhone_appVersion as STRING) as phone_versao_app,
  SAFE_CAST (infoPhone_phoneModel as STRING) as phone_modelo,
  SAFE_CAST (infoPhone_phoneManufacturer as STRING) as phone_fabricante,
  SAFE_CAST (infoPhone_osVersion as STRING) as phone_versao_sistema,
  SAFE_CAST (infoPhone_osName as STRING) as phone_nome_sistema,
  SAFE_CAST (tokeninfo_httpSalt as STRING) as ficha_http_aleatorio,
  SAFE_CAST (tokeninfo_wssSalt as STRING) as ficha_wss_aleatorio,
  SAFE_CAST (tokeninfo_pushToken as STRING) as ficha_envio,
  SAFE_CAST (
    associatedRace_originAtAccepted_position_lng as FLOAT64
  ) as corrida_origem_posicao_lng,
  SAFE_CAST (
    associatedRace_originAtAccepted_position_lat as FLOAT64
  ) as corrida_origem_posicao_lat,
  SAFE_CAST (associatedRace_race as STRING) as id_corrida,
  DATETIME (TIMESTAMP(expiredBlockByRankingDate)) as data_bloqueio_expirado,
  SAFE_CAST (ano_particao as INT64) as ano_particao,
  SAFE_CAST (mes_particao as INT64) as mes_particao,
  DATE(SAFE_CAST(ano_particao AS INT64), SAFE_CAST(mes_particao AS INT64), 1) AS data_particao
FROM
  `rj-iplanrio.brutos_taxirio_staging.drivers`
