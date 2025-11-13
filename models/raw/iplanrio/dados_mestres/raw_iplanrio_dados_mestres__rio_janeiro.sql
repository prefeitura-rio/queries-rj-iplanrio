{{
    config(
        alias="rio_janeiro",
        description="Dados do município do Rio de Janeiro"
    )
}}

SELECT
    "3304557" AS id_municipio,
    "RJ" AS sigla_uf,
    "Rio de Janeiro" AS nome_municipio,
    SAFE_CAST(REGEXP_REPLACE(st_area_shape_, r',', '.') AS FLOAT64) AS area,
    SAFE_CAST(REGEXP_REPLACE(st_perimeter_shape_, r',', '.') AS FLOAT64) AS perimetro,
    SAFE_CAST(geometry AS STRING) AS geometry_wkt,
    ST_GEOGFROMTEXT(geometry, make_valid => TRUE) AS geometry
FROM {{ source("brutos_dados_mestres_staging", "rio_janeiro") }}
