{{ config(
    materialized='table',
    alias='divisoes_administrativas',
    schema='rmi_dados_mestres',
    partition_by={
        "field": "tipo_divisao",
        "data_type": "string"
    },
    cluster_by=['ativo', 'tipo_divisao']
) }}

-- Divisões Administrativas do Rio de Janeiro
-- Consolida todas as divisões administrativas, políticas, educacionais e de planejamento 
-- em uma estrutura unificada sem hierarquia fixa

with municipio_geo as (
    select 
        geometria,
        ST_AsText(geometria) as geometry_wkt,
        ST_X(ST_Centroid(geometria)) as centroide_longitude,
        ST_Y(ST_Centroid(geometria)) as centroide_latitude,
        ST_Area(geometria) as area_m2,
        ST_Perimeter(geometria) as perimetro_m
    from `basedosdados.br_geobr_mapas.municipio`
    where id_municipio = '3304557'
),

municipio as (
    select
        'municipio_rio_de_janeiro' as id_divisao,
        'MUNICIPIO' as tipo_divisao,
        '3304557' as codigo_original,
        'Rio de Janeiro' as nome,
        'RJ' as nome_abreviado,
        'PCRJ' as orgao_responsavel,
        cast(['administracao_municipal'] as array<string>) as competencias,
        cast(null as date) as data_criacao,
        'Lei Orgânica do Município' as legislacao_base,
        geo.area_m2,
        geo.perimetro_m,
        geo.centroide_latitude,
        geo.centroide_longitude,
        geo.geometria as geometry,
        geo.geometry_wkt,
        5265.82 as densidade_populacional,
        'urbano' as uso_solo_predominante,
        cast([] as array<string>) as restricoes_urbanisticas,
        cast([] as array<string>) as instrumentos_urbanisticos,
        to_json(struct(
            6748000 as populacao,
            round(geo.area_m2 / 1000000, 2) as area_km2,
            'Sudeste' as regiao,
            'Rio de Janeiro' as estado
        )) as atributos_especificos,
        to_json(struct(
            'dados_ibge' as fonte,
            'basedosdados_geobr' as fonte_geometria
        )) as metadados_fonte,
        true as ativo,
        current_timestamp() as data_atualizacao,
        'basedosdados' as fonte_dados,
        '1.0' as versao_schema
    from municipio_geo geo
),

bairros as (
    select * from {{ ref('int_divisoes_bairros') }}
),

areas_planejamento as (
    select * from {{ ref('int_divisoes_areas_planejamento') }}
),

subprefeituras as (
    select * from {{ ref('int_divisoes_subprefeituras') }}
),

cres as (
    select * from {{ ref('int_divisoes_cres') }}
),

aeis as (
    select * from {{ ref('int_divisoes_aeis') }}
)

select * from municipio
union all
select * from areas_planejamento
union all  
select * from bairros
union all
select * from subprefeituras
union all
select * from cres
union all
select * from aeis