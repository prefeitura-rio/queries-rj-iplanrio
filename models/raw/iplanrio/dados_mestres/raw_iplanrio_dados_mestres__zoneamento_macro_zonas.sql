{{
    config(
        alias="zoneamento_macro_zonas",
        description="Dados de Macro Zonas de Zoneamento do município do Rio de Janeiro"
    )
}}

SELECT
    SAFE_CAST(TRIM(macrozona) AS STRING) AS nome_macrozona,
    SAFE_CAST(REGEXP_REPLACE(st_area_shape_, r',', '.') AS FLOAT64) AS area,
    SAFE_CAST(REGEXP_REPLACE(st_length_shape_, r',', '.') AS FLOAT64) AS perimetro,
    SAFE_CAST(geometry AS STRING) AS geometry_wkt,
    ST_GEOGFROMTEXT(geometry, make_valid => TRUE) AS geometry
FROM {{ source("brutos_dados_mestres_staging", "zoneamento_macro_zonas") }}
