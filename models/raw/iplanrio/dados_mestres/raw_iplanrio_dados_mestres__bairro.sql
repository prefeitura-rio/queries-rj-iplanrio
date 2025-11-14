{{
    config(
        alias="bairro",
        description="Dados de bairros do município do Rio de Janeiro"
    )
}}

WITH t as (
SELECT 
    SAFE_CAST(REGEXP_REPLACE(LTRIM(codbairro ,'0') , r'\.0$', '') AS STRING) id_bairro,
    SAFE_CAST(RTRIM(nome) AS STRING) nome,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(area_plane ,'0'), r'\.0$', '') AS STRING) id_area_planejamento,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(cod_rp ,'0'), r'\.0$', '') AS STRING) id_regiao_planejamento,
    SAFE_CAST(rp AS STRING) nome_regiao_planejamento,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(codra ,'0'), r'\.0$', '') AS STRING) id_regiao_administrativa,
    SAFE_CAST(INITCAP(RTRIM(regiao_adm)) AS STRING) nome_regiao_administrativa,
    SAFE_CAST(st_area_shape_ AS FLOAT64) area,
    SAFE_CAST(st_perimeter_shape_ AS FLOAT64) perimetro, 
    SAFE_CAST(geometry AS STRING) geometry_wkt,
    ST_GEOGFROMTEXT(geometry, make_valid => TRUE) geometry 
FROM {{ source("brutos_dados_mestres_staging", "bairro") }}
), 

subprefeituras_join as (
SELECT 
  id_bairro,
  t.nome,
  t.id_area_planejamento,
  t.id_regiao_planejamento,
  t.nome_regiao_planejamento,
  t.id_regiao_administrativa,
  t.nome_regiao_administrativa,
  CASE 
    WHEN t.nome="Paquetá" THEN "Ilhas do Governador/Fundão/Paquetá" 
    WHEN t.nome="Benfica" THEN "Centro" 
    WHEN t.nome="Cidade Universitária" THEN "Ilhas do Governador/Fundão/Paquetá" 
    WHEN t.nome="Gávea" THEN "Zona Sul"
    WHEN t.nome="Ipanema" THEN "Zona Sul" 
  ELSE sub.nome_subprefeitura END subprefeitura,
  -- t2.nome subprefeitura,
  t.area,
  t.perimetro,
  t.geometry_wkt,
  t.geometry
FROM t
LEFT JOIN {{ source("brutos_dados_mestres_staging", "subprefeitura") }} as sub 
  ON ST_CONTAINS(SAFE.ST_GEOGFROMTEXT(sub.geometry), ST_CENTROID(t.geometry))
-- LEFT JOIN `rj-escritorio-dev.dados_mestres.subprefeituras_regiao_adm` t2
--   ON t.id_regiao_administrativa = cast(t2.id_regiao_administrativa as string))
)


-- As subprefeituras que constam no mapa do Metabase são:
-- Centro, Ilhas, Zona Sul, Grande Tijuca, Zona Norte, Jacarepaguá, Barra Da Tijuca e Zona Oeste
SELECT
  id_bairro,
  nome,
  id_area_planejamento,
  id_regiao_planejamento,
  nome_regiao_planejamento,
  id_regiao_administrativa,
  nome_regiao_administrativa,
  CASE
    WHEN subprefeitura="Ilhas do Governador/Fundão/Paquetá" THEN "Ilhas" 
    WHEN subprefeitura="Benfica" THEN "Centro" 
    WHEN subprefeitura="Tijuca" THEN "Grande Tijuca"
    WHEN subprefeitura="Centro e Centro Histórico" THEN "Centro"
  ELSE subprefeitura END subprefeitura,
  area,
  perimetro,
  geometry_wkt,
  geometry
FROM subprefeituras_join
