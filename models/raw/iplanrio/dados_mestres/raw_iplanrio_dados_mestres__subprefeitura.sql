{{ config(
    materialized='table'
) }}

with source as (
    select * from {{ source('dados_mestres', 'subprefeitura') }}
),
renamed as (
    select
        subprefeitura,
        area,
        perimetro,
        geometria,
        geometry_wkt
    from source
)
select * from renamed