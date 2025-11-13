{{
    config(
        alias="lote",
        description="Dados de lotes do município do Rio de Janeiro"
    )
}}

SELECT
    SAFE_CAST(REGEXP_REPLACE(LTRIM(cod_lote, '0'), r'\.0$', '') AS STRING) AS id_lote,
    SAFE_CAST(TRIM(obs) AS STRING) AS observacao,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(cod_quadra, '0'), r'\.0$', '') AS STRING) AS id_quadra,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(cod_np, '0'), r'\.0$', '') AS STRING) AS id_numero_porta,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(cod_trecho, '0'), r'\.0$', '') AS STRING) AS id_trecho,
    SAFE_CAST(REGEXP_REPLACE(shape__area, r',', '.') AS FLOAT64) AS area,
    SAFE_CAST(REGEXP_REPLACE(shape__length, r',', '.') AS FLOAT64) AS perimetro,
    SAFE_CAST(geometry AS STRING) AS geometry_wkt,
    ST_GEOGFROMTEXT(geometry, make_valid => TRUE) AS geometry
FROM {{ source("brutos_dados_mestres_staging", "lote") }}
