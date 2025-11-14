{{
    config(
        alias="regiao_planejamento",
        description="Dados de Regiões de Planejamento (RP) do município do Rio de Janeiro"
    )
}}

SELECT
    SAFE_CAST(REGEXP_REPLACE(LTRIM(rp, '0'), r'\.0$', '') AS STRING) AS id_regiao_planejamento,
    SAFE_CAST(TRIM(nome_rp) AS STRING) AS nome,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(ap, '0'), r'\.0$', '') AS STRING) AS id_area_planejamento,
    SAFE_CAST(REGEXP_REPLACE(st_area_shape_, r',', '.') AS FLOAT64) AS area,
    SAFE_CAST(REGEXP_REPLACE(st_perimeter_shape_, r',', '.') AS FLOAT64) AS perimetro,
    SAFE_CAST(geometry AS STRING) AS geometry_wkt,
    ST_GEOGFROMTEXT(geometry, make_valid => TRUE) AS geometry
FROM {{ source("brutos_dados_mestres_staging", "regiao_planejamento") }}
