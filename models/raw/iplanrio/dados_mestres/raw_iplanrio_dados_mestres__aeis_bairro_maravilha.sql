{{
    config(
        alias="aeis_bairro_maravilha",
        description="Dados de Áreas de Especial Interesse Social (AEIS) de Bairro Maravilha do município do Rio de Janeiro"
    )
}}

SELECT
    SAFE_CAST(TRIM(name) AS STRING) AS nome,
    SAFE_CAST(TRIM(folderpath) AS STRING) AS caminho_pasta,
    SAFE_CAST(symbolid AS INT64) AS id_simbolo,
    SAFE_CAST(TRIM(altmode) AS STRING) AS modo_altitude,
    SAFE_CAST(TRIM(base) AS STRING) AS base,
    SAFE_CAST(TRIM(clamped) AS STRING) AS fixado,
    SAFE_CAST(TRIM(extruded) AS STRING) AS extrudado,
    SAFE_CAST(TRIM(snippet) AS STRING) AS trecho,
    SAFE_CAST(TRIM(popupinfo) AS STRING) AS informacao_popup,
    SAFE_CAST(TRIM(bairro) AS STRING) AS nome_bairro,
    SAFE_CAST(TRIM(sequencia) AS STRING) AS sequencia,
    SAFE_CAST(TRIM(nome) AS STRING) AS nome_aeis,
    SAFE_CAST(TRIM(legislacao) AS STRING) AS legislacao,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(codigo, '0'), r'\.0$', '') AS STRING) AS id_aeis_bairro_maravilha,
    SAFE_CAST(TRIM(tipo) AS STRING) AS tipo,
    SAFE_CAST(REGEXP_REPLACE(shape__length, r',', '.') AS FLOAT64) AS comprimento,
    SAFE_CAST(geometry AS STRING) AS geometry_wkt,
    ST_GEOGFROMTEXT(geometry, make_valid => TRUE) AS geometry
FROM {{ source("brutos_dados_mestres_staging", "aeis_bairro_maravilha") }}
