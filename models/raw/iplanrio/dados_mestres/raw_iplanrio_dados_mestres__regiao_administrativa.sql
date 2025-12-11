{{
    config(
        alias="regiao_administrativa",
        description="Dados de Regiões Administrativas (RA) do município do Rio de Janeiro"
    )
}}

SELECT
    SAFE_CAST(REGEXP_REPLACE(LTRIM(codra, '0'), r'\.0$', '') AS STRING) AS id_regiao_administrativa,
    SAFE_CAST(TRIM(nomera) AS STRING) AS nome,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(codap, '0'), r'\.0$', '') AS STRING) AS id_area_planejamento,
    SAFE_CAST(codapnum AS INT64) AS id_area_planejamento_numerico,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(cod_ap_sms, '0'), r'\.0$', '') AS STRING) AS id_area_planejamento_sms,
    SAFE_CAST(REGEXP_REPLACE(sum_area, r',', '.') AS FLOAT64) AS area_total,
    SAFE_CAST(REGEXP_REPLACE(st_area_shape_, r',', '.') AS FLOAT64) AS area,
    SAFE_CAST(REGEXP_REPLACE(st_perimeter_shape_, r',', '.') AS FLOAT64) AS perimetro,
    SAFE_CAST(geometry AS STRING) AS geometry_wkt,
    ST_GEOGFROMTEXT(geometry, make_valid => TRUE) AS geometry
FROM {{ source("brutos_dados_mestres_staging", "regiao_administrativa") }}
