{{ config(
    materialized='table'
) }}

with source as (
    select * from {{ source('dados_mestres', 'bairro') }}
),
renamed as (
    select
        id_bairro,
        nome,
        id_area_planejamento,
        nome_regiao_planejamento,
        id_regiao_administrativa,
        nome_regiao_administrativa,
        subprefeitura,
        area,
        perimetro,
        geometry,
        geometry_wkt
    from source
)
select * from renamed