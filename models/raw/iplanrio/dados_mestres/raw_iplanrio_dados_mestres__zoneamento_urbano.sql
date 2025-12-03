{{
    config(
        alias="zoneamento_urbano",
        description="Dados de Zoneamento Urbano do município do Rio de Janeiro"
    )
}}

SELECT
    SAFE_CAST(TRIM(legislacao) AS STRING) AS legislacao,
    SAFE_CAST(TRIM(zona) AS STRING) AS nome_zona,
    SAFE_CAST(TRIM(subzona) AS STRING) AS nome_subzona,
    SAFE_CAST(TRIM(sigla) AS STRING) AS sigla,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(ap, '0'), r'\.0$', '') AS STRING) AS id_area_planejamento,
    SAFE_CAST(TRIM(cab) AS STRING) AS coeficiente_aproveitamento_basico,
    SAFE_CAST(TRIM(obs_cab) AS STRING) AS observacao_coeficiente_aproveitamento_basico,
    SAFE_CAST(TRIM(cam) AS STRING) AS coeficiente_aproveitamento_maximo,
    SAFE_CAST(TRIM(obs_cam) AS STRING) AS observacao_coeficiente_aproveitamento_maximo,
    SAFE_CAST(TRIM(to_) AS STRING) AS taxa_ocupacao,
    SAFE_CAST(TRIM(obs_to) AS STRING) AS observacao_taxa_ocupacao,
    SAFE_CAST(TRIM(lote_min) AS STRING) AS lote_minimo,
    SAFE_CAST(TRIM(obs_lote_min) AS STRING) AS observacao_lote_minimo,
    SAFE_CAST(TRIM(testada_min) AS STRING) AS testada_minima,
    SAFE_CAST(TRIM(obs_testada_min) AS STRING) AS observacao_testada_minima,
    SAFE_CAST(TRIM(gab_afast) AS STRING) AS gabarito_afastamento,
    SAFE_CAST(TRIM(obs_gab_afast) AS STRING) AS observacao_gabarito_afastamento,
    SAFE_CAST(TRIM(gab_n_afast) AS STRING) AS gabarito_nao_afastamento,
    SAFE_CAST(TRIM(obs_gab_n_afast) AS STRING) AS observacao_gabarito_nao_afastamento,
    SAFE_CAST(TRIM(afast_fron) AS STRING) AS afastamento_frontal,
    SAFE_CAST(TRIM(obs_afast_fron) AS STRING) AS observacao_afastamento_frontal,
    SAFE_CAST(TRIM(ics) AS STRING) AS indice_cobertura_solo,
    SAFE_CAST(TRIM(obs_ics) AS STRING) AS observacao_indice_cobertura_solo,
    SAFE_CAST(TRIM(obs_riu) AS STRING) AS observacao_regiao_interesse_urbanistico,
    SAFE_CAST(TRIM(obs) AS STRING) AS observacao,
    SAFE_CAST(TRIM(created_user) AS STRING) AS usuario_criacao,
    SAFE_CAST(created_date AS DATE) AS data_criacao,
    SAFE_CAST(TRIM(last_edited_user) AS STRING) AS usuario_ultima_edicao,
    SAFE_CAST(last_edited_date AS DATE) AS data_ultima_edicao,
    SAFE_CAST(REGEXP_REPLACE(st_area_shape_, r',', '.') AS FLOAT64) AS area,
    SAFE_CAST(REGEXP_REPLACE(st_length_shape_, r',', '.') AS FLOAT64) AS perimetro,
    SAFE_CAST(geometry AS STRING) AS geometry_wkt,
    ST_GEOGFROMTEXT(geometry, make_valid => TRUE) AS geometry
FROM {{ source("brutos_dados_mestres_staging", "zoneamento_urbano") }}
