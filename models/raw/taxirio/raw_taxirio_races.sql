with
    source as (select * from {{ source("brutos_taxirio_staging", "races") }}),
    renamed as (
        select
            id,
            createdat,
            event,
            passenger,
            city,
            broadcastqtd,
            rating_score,
            issuspect,
            isinvalid,
            status,
            car,
            driver,
            routeorigindestination_distance_text,
            routeorigindestination_distance_value,
            routeorigindestination_duration_text,
            routeorigindestination_duration_value,
            billing_estimatedprice,
            billing_associatedpaymentmethod,
            billing_associatedtaximeter,
            billing_associatedminimumfare,
            billing_associateddiscount,
            billing_associatedcorporative_externalpropertypassenger,
            billing_finalprice_totaltopay,
            billing_finalprice_totalpricetoll,
            billing_finalprice_totalwithdiscount,
            billing_finalprice_totaldiscount,
            billing_finalprice_totalwithoutdiscount,
            billing_finalprice_totalbytaximeter,
            billing_finalprice_totalbystoppedtime,
            billing_finalprice_totalbykm,
            billing_finalprice_minprice,
            ano_particao,
            mes_particao,
            dia_particao
        from source
    )

select *
from renamed
