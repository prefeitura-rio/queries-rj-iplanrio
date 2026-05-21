{{
    config(
        alias="bairro", description="Dados de bairros do município do Rio de Janeiro"
    )
}}

with
    t as (
        select
            safe_cast(
                regexp_replace(ltrim(codbairro, '0'), r'\.0$', '') as string
            ) id_bairro,
            safe_cast(rtrim(nome) as string) nome,
            safe_cast(
                regexp_replace(ltrim(area_plane, '0'), r'\.0$', '') as string
            ) id_area_planejamento,
            safe_cast(
                regexp_replace(ltrim(cod_rp, '0'), r'\.0$', '') as string
            ) id_regiao_planejamento,
            safe_cast(rp as string) nome_regiao_planejamento,
            safe_cast(
                regexp_replace(ltrim(codra, '0'), r'\.0$', '') as string
            ) id_regiao_administrativa,
            safe_cast(initcap(rtrim(regiao_adm)) as string) nome_regiao_administrativa,
            safe_cast(st_area_shape_ as float64) area,
            safe_cast(st_perimeter_shape_ as float64) perimetro,
            safe_cast(geometry as string) geometry_wkt,
            st_geogfromtext(geometry, make_valid => true) geometry
        from {{ source("brutos_dados_mestres_staging", "bairro") }}
    ),

    subprefeituras_join as (
        select
            id_bairro,
            t.nome,
            t.id_area_planejamento,
            t.id_regiao_planejamento,
            t.nome_regiao_planejamento,
            t.id_regiao_administrativa,
            t.nome_regiao_administrativa,
            case
                when t.nome = "Paquetá"
                then "Ilhas do Governador/Fundão/Paquetá"
                when t.nome = "Benfica"
                then "Centro"
                when t.nome = "Cidade Universitária"
                then "Ilhas do Governador/Fundão/Paquetá"
                when t.nome = "Gávea"
                then "Zona Sul"
                when t.nome = "Ipanema"
                then "Zona Sul"
                else sub.subprefeitura
            end subprefeitura,
            -- t2.nome subprefeitura,
            t.area,
            t.perimetro,
            t.geometry_wkt,
            t.geometry
        from t
        left join
            {{ source("brutos_dados_mestres_staging", "subprefeitura") }} as sub
            on st_contains(
                st_geogfromtext(sub.geometry, make_valid => true),
                st_centroid(t.geometry)
            )
    -- LEFT JOIN `rj-escritorio-dev.dados_mestres.subprefeituras_regiao_adm` t2
    -- ON t.id_regiao_administrativa = cast(t2.id_regiao_administrativa as string))
    )

-- As subprefeituras que constam no mapa do Metabase são:
-- Centro, Ilhas, Zona Sul, Grande Tijuca, Zona Norte, Jacarepaguá, Barra Da Tijuca e
-- Zona Oeste
select
    id_bairro,
    nome,
    id_area_planejamento,
    id_regiao_planejamento,
    nome_regiao_planejamento,
    id_regiao_administrativa,
    nome_regiao_administrativa,
    case
        when subprefeitura = "Ilhas do Governador/Fundão/Paquetá"
        then "Ilhas"
        when subprefeitura = "Benfica"
        then "Centro"
        when subprefeitura = "Tijuca"
        then "Grande Tijuca"
        when subprefeitura = "Centro e Centro Histórico"
        then "Centro"
        else subprefeitura
    end subprefeitura,
    area,
    perimetro,
    geometry_wkt,
    geometry
from subprefeituras_join