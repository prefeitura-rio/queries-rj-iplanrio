with source as (
        select * from {{ source('brutos_taxirio_staging', 'races') }}
  ),
  renamed as (
      select
          {{ adapter.quote("id") }},
        {{ adapter.quote("createdAt") }},
        {{ adapter.quote("event") }},
        {{ adapter.quote("passenger") }},
        {{ adapter.quote("city") }},
        {{ adapter.quote("broadcastQtd") }},
        {{ adapter.quote("rating_score") }},
        {{ adapter.quote("isSuspect") }},
        {{ adapter.quote("isInvalid") }},
        {{ adapter.quote("status") }},
        {{ adapter.quote("car") }},
        {{ adapter.quote("driver") }},
        {{ adapter.quote("routeOriginDestination_distance_text") }},
        {{ adapter.quote("routeOriginDestination_distance_value") }},
        {{ adapter.quote("routeOriginDestination_duration_text") }},
        {{ adapter.quote("routeOriginDestination_duration_value") }},
        {{ adapter.quote("billing_estimatedPrice") }},
        {{ adapter.quote("billing_associatedPaymentMethod") }},
        {{ adapter.quote("billing_associatedTaximeter") }},
        {{ adapter.quote("billing_associatedMinimumFare") }},
        {{ adapter.quote("billing_associatedDiscount") }},
        {{ adapter.quote("billing_associatedCorporative_externalPropertyPassenger") }},
        {{ adapter.quote("billing_finalPrice_totalToPay") }},
        {{ adapter.quote("billing_finalPrice_totalPriceToll") }},
        {{ adapter.quote("billing_finalPrice_totalWithDiscount") }},
        {{ adapter.quote("billing_finalPrice_totalDiscount") }},
        {{ adapter.quote("billing_finalPrice_totalWithoutDiscount") }},
        {{ adapter.quote("billing_finalPrice_totalByTaximeter") }},
        {{ adapter.quote("billing_finalPrice_totalByStoppedTime") }},
        {{ adapter.quote("billing_finalPrice_totalByKm") }},
        {{ adapter.quote("billing_finalPrice_minPrice") }},
        {{ adapter.quote("ano_particao") }},
        {{ adapter.quote("mes_particao") }},
        {{ adapter.quote("dia_particao") }}

      from source
  )
  select * from renamed
    