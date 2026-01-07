{{
    config(
        alias= 'eai_gateway_uptime', 
        schema='brutos_betterstack', 
        materialized="table",
        cluster_by="id"
    )
}}

SELECT * FROM `rj-iplanrio.brutos_betterstack_staging.eai_gateway_incidents` LIMIT 1000


