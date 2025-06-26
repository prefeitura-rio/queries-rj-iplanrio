{{
    config(
      schema="brutos_taxirio",
      alias="motoristas",
      materialized="table",
      partition_by={
        "field": "data_criacao_particao",
        "data_type": "timestamp",
        "granularity": "day"
      },
      tags=["raw", "taxirio"],
      description="Tabela de Motoristas"
    )
}}
SELECT
  safe_cast(id as string) as id_motorista,
  datetime(timestamp(createdAt)) as data_criacao,
  datetime(timestamp(createdAt)) as data_criacao_particao,
  safe_cast(user as string) as id_usuario,
  safe_cast(taxiDriverId as string) as id_motorista_taxi,
  safe_cast(cars as string) as id_carro_taxi,
  safe_cast(average as float64) as nota_media,
  safe_cast(associatedCar as string) as id_carro_associado,
  safe_cast(status as string) as status,
  safe_cast(associatedDiscount as string) as id_desconto_associado,
  parse_json(associatedPaymentsMethods) as pagamento_metodo,
  safe_cast(login as string) as usuario,
  safe_cast(password as string) as senha,
  safe_cast(salt as string) as dado_aleatorio,
  safe_cast(isAbleToReceivePaymentInApp as bool) as pode_receber_pagamento_app,
  safe_cast(isAbleToReceivePaymentInCityHall as bool) as pode_receber_pagamento_prefeitura,
  safe_cast(ratingsReceived as int64) as avaliacoes_recebidas,
  safe_cast(busy as bool) as ocupado,
  safe_cast(lastAverage as float64) as ultima_nota_media,
  datetime(timestamp(expiredBlockByRankingDate)) as data_termino_classificacao_bloqueio,
  safe_cast(blockedRace as string) as id_corrida_bloqueada,
  safe_cast(city as string) as id_municipio,
  safe_cast(serviceRecordRate as float64) as taxa_servico,
  safe_cast(nota as float64) as nota_passageiro,
  safe_cast(averageTT as float64) as mediatt,
  datetime(timestamp(infoPhone_updatedAt)) as atualizacao_telefone,
  safe_cast(infoPhone_id as string) as id_telefone,
  safe_cast(infoPhone_appVersion as string) as versao_app_telefone,
  safe_cast(infoPhone_phoneModel as string) as modelo_telefone,
  safe_cast(infoPhone_phoneManufacturer as string) as fabricante_telefone,
  safe_cast(infoPhone_osVersion as string) as versao_sistema_telefone,
  safe_cast(infoPhone_osName as string) as nome_sistema_telefone,
  safe_cast(tokeninfo_httpSalt as string) as ficha_http_aleatorio,
  safe_cast(tokeninfo_wssSalt as string) as ficha_wss_aleatorio,
  safe_cast(tokeninfo_pushToken as string) as ficha_envio,
  safe_cast(associatedRace_originAtAccepted_position_lng as float64) as corrida_origem_posicao_lng,
  safe_cast(associatedRace_originAtAccepted_position_lat as float64) as corrida_origem_posicao_lat,
  safe_cast(associatedRace_race as string) as id_corrida,
  datetime(timestamp(expiredBlockByRankingDate)) as data_bloqueio_expirado,

FROM
  {{ source('brutos_taxirio_staging','drivers') }}
