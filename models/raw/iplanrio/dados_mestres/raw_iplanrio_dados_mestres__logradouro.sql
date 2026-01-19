{{
    config(
        alias="logradouro",
        description="Dados de logradouros (ruas) do município do Rio de Janeiro"
    )
}}

SELECT
    SAFE_CAST(REGEXP_REPLACE(LTRIM(cod_trecho, '0'), r'\.0$', '') AS STRING) AS id_trecho,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(cl, '0'), r'\.0$', '') AS STRING) AS id_logradouro,
    SAFE_CAST(np_ini_par AS FLOAT64) AS inicio_numero_porta_par,
    SAFE_CAST(np_fin_par AS FLOAT64) AS final_numero_porta_par,
    SAFE_CAST(np_ini_imp AS FLOAT64) AS inicio_numero_porta_impar,
    SAFE_CAST(np_fin_imp AS FLOAT64) AS final_numero_porta_impar,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(cod_tipo_logra, '0'), r'\.0$', '') AS STRING) AS id_tipo_logradouro,
    SAFE_CAST(TRIM(tipo_logra_abr) AS STRING) AS tipo_logradouro_abreviado,
    SAFE_CAST(TRIM(tipo_logra_ext) AS STRING) AS tipo_logradouro_extenso,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(cod_nobreza, '0'), r'\.0$', '') AS STRING) AS id_nobreza,
    SAFE_CAST(TRIM(nobreza) AS STRING) AS nobreza,
    SAFE_CAST(TRIM(preposicao) AS STRING) AS preposicao,
    SAFE_CAST(TRIM(nome_parcial) AS STRING) AS nome_parcial,
    SAFE_CAST(TRIM(completo) AS STRING) AS nome_completo,
    SAFE_CAST(TRIM(nome_mapa) AS STRING) AS nome_mapa,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(cod_bairro, '0'), r'\.0$', '') AS STRING) AS id_bairro,
    SAFE_CAST(TRIM(bairro) AS STRING) AS nome_bairro,
    SAFE_CAST(TRIM(hierarquia) AS STRING) AS hierarquia,
    SAFE_CAST(TRIM(oneway) AS STRING) AS sentido_unico,
    SAFE_CAST(TRIM(velocidade_regulamentada) AS STRING) AS velocidade_regulamentada,
    SAFE_CAST(TRIM(tipo_trecho) AS STRING) AS tipo_trecho,
    SAFE_CAST(last_edited_date AS DATE) AS data_ultima_edicao,
    SAFE_CAST(geometry AS STRING) AS geometry_wkt,
    ST_GEOGFROMTEXT(geometry, make_valid => TRUE) AS geometry
FROM {{ source("brutos_dados_mestres_staging", "logradouro") }}
