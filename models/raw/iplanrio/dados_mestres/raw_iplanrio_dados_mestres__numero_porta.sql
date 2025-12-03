{{
    config(
        alias="numero_porta",
        description="Dados de números de porta (endereços) do município do Rio de Janeiro"
    )
}}

SELECT
    SAFE_CAST(REGEXP_REPLACE(LTRIM(cod_np, '0'), r'\.0$', '') AS STRING) AS id_numero_porta,
    SAFE_CAST(np AS INT64) AS numero_porta,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(cl, '0'), r'\.0$', '') AS STRING) AS id_logradouro,
    SAFE_CAST(TRIM(clnp) AS STRING) AS codigo_logradouro_numero_porta,
    SAFE_CAST(TRIM(compl_np) AS STRING) AS complemento_numero_porta,
    SAFE_CAST(TRIM(clnp_ass) AS STRING) AS codigo_logradouro_numero_porta_associado,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(cod_uso, '0'), r'\.0$', '') AS STRING) AS id_uso,
    SAFE_CAST(TRIM(nome_grupo) AS STRING) AS nome_grupo,
    SAFE_CAST(TRIM(num_adj_a) AS STRING) AS numero_adjacente_a,
    SAFE_CAST(TRIM(num_adj_b) AS STRING) AS numero_adjacente_b,
    SAFE_CAST(data_inc AS DATE) AS data_inclusao,
    SAFE_CAST(data_int AS DATE) AS data_integracao,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(chavegeo_np, '0'), r'\.0$', '') AS STRING) AS id_chavegeo,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(cod_tipologia, '0'), r'\.0$', '') AS STRING) AS id_tipologia,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(cod_utilizacao, '0'), r'\.0$', '') AS STRING) AS id_utilizacao,
    SAFE_CAST(num_pavimentos AS INT64) AS numero_pavimentos,
    SAFE_CAST(flag_ed_ruinas AS BOOL) AS flag_edificacao_ruinas,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(cod_quadra, '0'), r'\.0$', '') AS STRING) AS id_quadra,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(cod_face, '0'), r'\.0$', '') AS STRING) AS id_face,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(cod_lote, '0'), r'\.0$', '') AS STRING) AS id_lote,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(cod_trecho, '0'), r'\.0$', '') AS STRING) AS id_trecho,
    SAFE_CAST(TRIM(y) AS STRING) AS y,
    SAFE_CAST(TRIM(cep) AS STRING) AS cep,
    SAFE_CAST(TRIM(clnp_trib) AS STRING) AS codigo_logradouro_numero_porta_tributario,
    SAFE_CAST(TRIM(x) AS STRING) AS x,
    SAFE_CAST(np_int AS INT64) AS numero_porta_inteiro,
    SAFE_CAST(latitude AS FLOAT64) AS latitude,
    SAFE_CAST(longitude AS FLOAT64) AS longitude,
    SAFE_CAST(geometry AS STRING) AS geometry_wkt,
    ST_GEOGFROMTEXT(geometry, make_valid => TRUE) AS geometry
FROM {{ source("brutos_dados_mestres_staging", "numero_porta") }}
