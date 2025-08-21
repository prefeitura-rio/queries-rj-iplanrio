{{
    config(
        alias="corridas",
        description="Corridas em andamento ou já encerradas.",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data_criacao_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}
with
    source as (select * from {{ source("brutos_taxirio_staging", "races") }}),

    renamed as (
        select
            safe_cast(id as string) as id_corrida,
            datetime(timestamp(createdat)) as data_criacao,
            cast(timestamp(createdat) as date) as data_criacao_particao,
            safe_cast(event as string) as id_evento,
            safe_cast(passenger as string) as id_passageiro,
            safe_cast(city as string) as id_municipio,
            safe_cast(
                billing_associatedpaymentmethod as string
            ) as id_pagamento_associado,
            safe_cast(broadcastqtd as int64) as qtd_transmissao,
            safe_cast(rating_score as int64) as nota_avaliacao,
            safe_cast(issuspect as bool) as suspeito,
            safe_cast(isinvalid as bool) as invalido,
            safe_cast(status as string) as status,
            safe_cast(car as string) as id_carro,
            safe_cast(driver as string) as id_motorista,
            safe_cast(
                routeorigindestination_distance_text as string
            ) as distancia_rota_texto,
            safe_cast(
                routeorigindestination_distance_value as int64
            ) as distancia_rota_valor,
            safe_cast(
                routeorigindestination_duration_text as string
            ) as duracao_rota_texto,
            safe_cast(
                routeorigindestination_duration_value as int64
            ) as duracao_rota_valor,
            safe_cast(billing_estimatedprice as float64) as valor_estimado,
            safe_cast(billing_associatedtaximeter as string) as id_taximetro_associado,
            safe_cast(
                billing_associatedminimumfare as string
            ) as id_tarifa_minima_associada,
            safe_cast(billing_associateddiscount as string) as id_desconto_associado,
            safe_cast(
                billing_associatedcorporative_externalpropertypassenger as string
            ) as id_desconto_passageiro,
            safe_cast(
                replace(billing_finalprice_totaltopay, ',', '.') as float64
            ) as valor_total_a_pagar,
            safe_cast(
                replace(billing_finalprice_totalpricetoll, ',', '.') as float64
            ) as valor_total_pedagio,
            safe_cast(
                replace(billing_finalprice_totalwithdiscount, ',', '.') as float64
            ) as valor_total_com_desconto,
            safe_cast(
                replace(billing_finalprice_totaldiscount, ',', '.') as float64
            ) as valor_total_desconto,
            safe_cast(
                replace(billing_finalprice_totalwithoutdiscount, ',', '.') as float64
            ) as valor_total_sem_desconto,
            safe_cast(
                replace(billing_finalprice_totalbytaximeter, ',', '.') as float64
            ) as valor_total_por_taximetro,
            safe_cast(
                replace(billing_finalprice_totalbystoppedtime, ',', '.') as float64
            ) as valor_total_por_tempo_parado,
            safe_cast(
                replace(billing_finalprice_totalbykm, ',', '.') as float64
            ) as valor_total_por_km,
            safe_cast(
                replace(billing_finalprice_minprice, ',', '.') as float64
            ) as valor_minimo,
            safe_cast(
                billing_associatedcorporative_externalpropertypassenger as bool
            ) as propriedade_corporativa,
            _airbyte_extracted_at as datalake_loaded_at,    
            current_timestamp() as datalake_transformed_at      
        from source
    )

select *
from renamed
{% if is_incremental() %}
    where data_criacao_particao >= date_add(current_date(), interval -3 day)
{% endif %}

