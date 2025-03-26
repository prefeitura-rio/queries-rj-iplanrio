SELECT
  SAFE_CAST (ano_particao as INT64) as ano_particao,
  SAFE_CAST (mes_particao as INT64) as mes_particao,
  SAFE_CAST (dia_particao as INT64) as dia_particao,
  DATETIME (TIMESTAMP(createdAt)) as data_criacao,
  SAFE_CAST (id as STRING) as id_corrida,
  SAFE_CAST (event as STRING) as id_evento,
  SAFE_CAST (passenger as STRING) as id_passageiro,
  SAFE_CAST (city as STRING) as id_municipio,
  SAFE_CAST (billing_associatedPaymentMethod as STRING) as id_pagamento_associado,
  SAFE_CAST (broadcastQtd as INT64) as qtd_transmissao,
  SAFE_CAST (rating_score as INT64) as nota_avaliacao,
  SAFE_CAST (isSuspect as BOOL) as suspeito,
  SAFE_CAST (isInvalid as BOOL) as invalido,
  SAFE_CAST (status as STRING) as status,
  SAFE_CAST (routeOriginDestination_distance_value as INT64) as distancia_rota,
  SAFE_CAST (routeOriginDestination_duration_value as INT64) as duracao_rota,
  SAFE_CAST (billing_estimatedPrice as FLOAT64) as valor_estimado,
  SAFE_CAST (billing_associatedTaximeter as STRING) as id_taximetro_associado,
  SAFE_CAST (billing_associatedMinimumFare as STRING) as id_tarifa_minima_associada,
  SAFE_CAST (billing_associatedDiscount as STRING) as id_desconto_associado,
  SAFE_CAST (
    billing_associatedCorporative_externalPropertyPassenger as BOOL
  ) as propriedade_corporativa,
FROM
  `rj-iplanrio.transporte_rodoviario_taxirio_homologacao_staging.races`
