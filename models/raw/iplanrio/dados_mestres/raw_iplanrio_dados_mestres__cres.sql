{{ config(
    materialized='table'
) }}

with source as (
    select * from {{ source('dados_mestres', 'cres') }}
),
renamed as (
    select
        id,
        nome,
        geometry,
        geometry_wkt
    from source
)
select * from renamed