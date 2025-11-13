{{
    config(
        alias="subprefeitura",
        description="Dados de subprefeituras do município do Rio de Janeiro"
    )
}}

SELECT 
    SAFE_CAST(TRIM(nome_subprefeitura) AS STRING) AS subprefeitura,
    SAFE_CAST(REGEXP_REPLACE(shape_area, r',', '.') AS FLOAT64) AS area,
    SAFE_CAST(REGEXP_REPLACE(shape_length, r',', '.') AS FLOAT64) AS perimetro,
    SAFE_CAST(geometry AS STRING) geometry_wkt,
    ST_GEOGFROMTEXT(geometry, make_valid => TRUE) AS geometria,
FROM {{ source("brutos_dados_mestres_staging", "subprefeitura") }} AS t