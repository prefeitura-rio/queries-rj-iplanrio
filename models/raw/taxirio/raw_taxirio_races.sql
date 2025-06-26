{{
    config(     
        schema="brutos_taxirio",
        alias="corridas",
        materialized="table",
        tags=["raw", "taxirio"],
        description="Corridas em andamento ou já encerradas.",
        partition_by={
            "field": "data_criacao_particao",
            "data_type": "timestamp",
            "granularity": "day"
        },
    )
}}
SELECT
  safe_cast(id as string) as id_corrida,
  datetime(timestamp(createdAt)) as data_criacao,
  datetime(timestamp(createdAt)) as data_criacao_particao,
  safe_cast(event as string) as id_evento,
  safe_cast(passenger as string) as id_passageiro,
  safe_cast(city as string) as id_municipio,
  safe_cast(billing_associatedPaymentMethod as string) as id_pagamento_associado,
  safe_cast(broadcastQtd as int64) as qtd_transmissao,
  safe_cast(rating_score as int64) as nota_avaliacao,
  safe_cast(isSuspect as bool) as suspeito,
  safe_cast(isInvalid as bool) as invalido,
  safe_cast(status as string) as status,
  safe_cast(car as string) as id_carro,
  safe_cast(driver as string) as id_motorista,
  safe_cast(routeOriginDestination_distance_text as string) as distancia_rota_texto,
  safe_cast(routeOriginDestination_distance_value as int64) as distancia_rota_valor,
  safe_cast(routeOriginDestination_duration_text as string) as duracao_rota_texto,
  safe_cast(routeOriginDestination_duration_value as int64) as duracao_rota_valor,
  safe_cast(billing_estimatedPrice as float64) as valor_estimado,
  safe_cast(billing_associatedTaximeter as string) as id_taximetro_associado,
  safe_cast(billing_associatedMinimumFare as string) as id_tarifa_minima_associada,
  safe_cast(billing_associatedDiscount as string) as id_desconto_associado,
  safe_cast(billing_associatedCorporative_externalPropertyPassenger as string) as id_desconto_passageiro,
  safe_cast(
    replace(billing_finalPrice_totalToPay, ',', '.') as float64
  ) as valor_total_a_pagar,
  safe_cast(
    replace(billing_finalPrice_totalPriceToll, ',', '.') as float64
  ) as valor_total_pedagio,
  safe_cast(
    replace(billing_finalPrice_totalWithDiscount, ',', '.') as float64
  ) as valor_total_com_desconto,
  safe_cast(
    replace(billing_finalPrice_totalDiscount, ',', '.') as float64
  ) as valor_total_desconto,
  safe_cast(
    replace(billing_finalPrice_totalWithoutDiscount, ',', '.') as float64
  ) as valor_total_sem_desconto,
  safe_cast(
    replace(billing_finalPrice_totalByTaximeter, ',', '.') as float64
  ) as valor_total_por_taximetro,
  safe_cast(
    replace(billing_finalPrice_totalByStoppedTime, ',', '.') as float64
  ) as valor_total_por_tempo_parado,
  safe_cast(
    replace(billing_finalPrice_totalByKm, ',', '.') as float64
  ) as valor_total_por_km,
  safe_cast(
    replace(billing_finalPrice_minPrice, ',', '.') as float64
  ) as valor_minimo,
  safe_cast(
    billing_associatedCorporative_externalPropertyPassenger as bool
  ) as propriedade_corporativa,
   
FROM
  {{ source('brutos_taxirio_staging','races') }}
