{{ config(
    materialized='table'
) }}

with source as (
    select * from {{ source('dados_mestres', 'aeis') }}
),
renamed as (
    select
        id_aeis,
        nome_aeis,
        nome_sigla,
        tipologia,
        data_cadastro,
        legislacao,
        area,
        comprimento,
        geometry,
        geometria_wkt,
        item,
        nome_setor,
        nome_ar,
        planta_cadastral,
        projeto_loteamento,
        referencia,
        limite_pavimentos_permitido,
        taxa_ocupacao,
        indice_aproveitamento_terreno,
        coeficiente_adensamento,
        afastamento,
        nome_regiao_administrativa
    from source
)
select * from renamed