
{{
    config(
        alias='transferencia_familia',
        schema='protecao_social_cadunico',
        materialized="table",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        }
    )
}}


SELECT

    --column: chv_natural_prefeitura_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_antes_transferencia,

    --column: cod_destino_familia_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,11))
        END AS STRING
    ) AS id_familia_destino,

    --column: cod_destino_prefeitura_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,13))
        END AS STRING
    ) AS id_prefeitura_destino,

    --column: cod_est_cadastral_atual_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_estado_cadastro_transferencia,
    --column: cod_est_cadastral_atual_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^4$') THEN 'Excluido'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS estado_cadastro_transferencia,

    --column: cod_familiar_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_antes_transferencia,

    --column: cod_munic_ibge_origem_2_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,2))
        END AS STRING
    ) AS id_uf_transferencia,

    --column: cod_munic_ibge_origem_5_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,5))
        END AS STRING
    ) AS id_municipio_transferencia,

    --column: dat_transferencia_famt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_transferencia,

    --column: num_reg_arquivo_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_transferencia,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0601'
    AND SUBSTRING(text,38,2) = '16'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_antes_transferencia,

    --column: cod_destino_familia_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,11))
        END AS STRING
    ) AS id_familia_destino,

    --column: cod_destino_prefeitura_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,13))
        END AS STRING
    ) AS id_prefeitura_destino,

    --column: cod_est_cadastral_atual_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_estado_cadastro_transferencia,
    --column: cod_est_cadastral_atual_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^4$') THEN 'Excluido'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS estado_cadastro_transferencia,

    --column: cod_familiar_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_antes_transferencia,

    --column: cod_munic_ibge_origem_2_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,2))
        END AS STRING
    ) AS id_uf_transferencia,

    --column: cod_munic_ibge_origem_5_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,5))
        END AS STRING
    ) AS id_municipio_transferencia,

    --column: dat_transferencia_famt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_transferencia,

    --column: num_reg_arquivo_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_transferencia,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0603'
    AND SUBSTRING(text,38,2) = '16'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_antes_transferencia,

    --column: cod_destino_familia_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,11))
        END AS STRING
    ) AS id_familia_destino,

    --column: cod_destino_prefeitura_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,13))
        END AS STRING
    ) AS id_prefeitura_destino,

    --column: cod_est_cadastral_atual_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_estado_cadastro_transferencia,
    --column: cod_est_cadastral_atual_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^4$') THEN 'Excluido'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS estado_cadastro_transferencia,

    --column: cod_familiar_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_antes_transferencia,

    --column: cod_munic_ibge_origem_2_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,2))
        END AS STRING
    ) AS id_uf_transferencia,

    --column: cod_munic_ibge_origem_5_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,5))
        END AS STRING
    ) AS id_municipio_transferencia,

    --column: dat_transferencia_famt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_transferencia,

    --column: num_reg_arquivo_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_transferencia,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0604'
    AND SUBSTRING(text,38,2) = '16'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_antes_transferencia,

    --column: cod_destino_familia_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,11))
        END AS STRING
    ) AS id_familia_destino,

    --column: cod_destino_prefeitura_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,13))
        END AS STRING
    ) AS id_prefeitura_destino,

    --column: cod_est_cadastral_atual_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_estado_cadastro_transferencia,
    --column: cod_est_cadastral_atual_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^4$') THEN 'Excluido'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS estado_cadastro_transferencia,

    --column: cod_familiar_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_antes_transferencia,

    --column: cod_munic_ibge_origem_2_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,2))
        END AS STRING
    ) AS id_uf_transferencia,

    --column: cod_munic_ibge_origem_5_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,5))
        END AS STRING
    ) AS id_municipio_transferencia,

    --column: dat_transferencia_famt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_transferencia,

    --column: num_reg_arquivo_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_transferencia,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0609'
    AND SUBSTRING(text,38,2) = '16'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_antes_transferencia,

    --column: cod_destino_familia_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,11))
        END AS STRING
    ) AS id_familia_destino,

    --column: cod_destino_prefeitura_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,13))
        END AS STRING
    ) AS id_prefeitura_destino,

    --column: cod_est_cadastral_atual_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_estado_cadastro_transferencia,
    --column: cod_est_cadastral_atual_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^4$') THEN 'Excluido'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS estado_cadastro_transferencia,

    --column: cod_familiar_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_antes_transferencia,

    --column: cod_munic_ibge_origem_2_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,2))
        END AS STRING
    ) AS id_uf_transferencia,

    --column: cod_munic_ibge_origem_5_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,5))
        END AS STRING
    ) AS id_municipio_transferencia,

    --column: dat_transferencia_famt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_transferencia,

    --column: num_reg_arquivo_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_transferencia,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0612'
    AND SUBSTRING(text,38,2) = '16'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_antes_transferencia,

    --column: cod_destino_familia_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,11))
        END AS STRING
    ) AS id_familia_destino,

    --column: cod_destino_prefeitura_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,13))
        END AS STRING
    ) AS id_prefeitura_destino,

    --column: cod_est_cadastral_atual_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_estado_cadastro_transferencia,
    --column: cod_est_cadastral_atual_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^4$') THEN 'Excluido'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS estado_cadastro_transferencia,

    --column: cod_familiar_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_antes_transferencia,

    --column: cod_munic_ibge_origem_2_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,2))
        END AS STRING
    ) AS id_uf_transferencia,

    --column: cod_munic_ibge_origem_5_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,5))
        END AS STRING
    ) AS id_municipio_transferencia,

    --column: dat_transferencia_famt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_transferencia,

    --column: num_reg_arquivo_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_transferencia,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0615'
    AND SUBSTRING(text,38,2) = '16'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_antes_transferencia,

    --column: cod_destino_familia_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,11))
        END AS STRING
    ) AS id_familia_destino,

    --column: cod_destino_prefeitura_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,13))
        END AS STRING
    ) AS id_prefeitura_destino,

    --column: cod_est_cadastral_atual_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_estado_cadastro_transferencia,
    --column: cod_est_cadastral_atual_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^4$') THEN 'Excluido'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS estado_cadastro_transferencia,

    --column: cod_familiar_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_antes_transferencia,

    --column: cod_munic_ibge_origem_2_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,2))
        END AS STRING
    ) AS id_uf_transferencia,

    --column: cod_munic_ibge_origem_5_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,5))
        END AS STRING
    ) AS id_municipio_transferencia,

    --column: dat_transferencia_famt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_transferencia,

    --column: num_reg_arquivo_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_transferencia,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0617'
    AND SUBSTRING(text,38,2) = '16'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_antes_transferencia,

    --column: cod_destino_familia_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,11))
        END AS STRING
    ) AS id_familia_destino,

    --column: cod_destino_prefeitura_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,13))
        END AS STRING
    ) AS id_prefeitura_destino,

    --column: cod_est_cadastral_atual_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_estado_cadastro_transferencia,
    --column: cod_est_cadastral_atual_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^4$') THEN 'Excluido'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS estado_cadastro_transferencia,

    --column: cod_familiar_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_antes_transferencia,

    --column: cod_munic_ibge_origem_2_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,2))
        END AS STRING
    ) AS id_uf_transferencia,

    --column: cod_munic_ibge_origem_5_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,5))
        END AS STRING
    ) AS id_municipio_transferencia,

    --column: dat_transferencia_famt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_transferencia,

    --column: num_reg_arquivo_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_transferencia,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0619'
    AND SUBSTRING(text,38,2) = '16'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_antes_transferencia,

    --column: cod_destino_familia_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,11))
        END AS STRING
    ) AS id_familia_destino,

    --column: cod_destino_prefeitura_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,13))
        END AS STRING
    ) AS id_prefeitura_destino,

    --column: cod_est_cadastral_atual_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_estado_cadastro_transferencia,
    --column: cod_est_cadastral_atual_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^4$') THEN 'Excluido'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS estado_cadastro_transferencia,

    --column: cod_familiar_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_antes_transferencia,

    --column: cod_munic_ibge_origem_2_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,2))
        END AS STRING
    ) AS id_uf_transferencia,

    --column: cod_munic_ibge_origem_5_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,5))
        END AS STRING
    ) AS id_municipio_transferencia,

    --column: dat_transferencia_famt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_transferencia,

    --column: num_reg_arquivo_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_transferencia,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0620'
    AND SUBSTRING(text,38,2) = '16'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_antes_transferencia,

    --column: cod_destino_familia_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,11))
        END AS STRING
    ) AS id_familia_destino,

    --column: cod_destino_prefeitura_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,13))
        END AS STRING
    ) AS id_prefeitura_destino,

    --column: cod_est_cadastral_atual_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_estado_cadastro_transferencia,
    --column: cod_est_cadastral_atual_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^4$') THEN 'Excluido'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS estado_cadastro_transferencia,

    --column: cod_familiar_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_antes_transferencia,

    --column: cod_munic_ibge_origem_2_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,2))
        END AS STRING
    ) AS id_uf_transferencia,

    --column: cod_munic_ibge_origem_5_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,5))
        END AS STRING
    ) AS id_municipio_transferencia,

    --column: dat_transferencia_famt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_transferencia,

    --column: num_reg_arquivo_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_transferencia,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0623'
    AND SUBSTRING(text,38,2) = '16'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_antes_transferencia,

    --column: cod_destino_familia_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,69,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,69,11))
        END AS STRING
    ) AS id_familia_destino,

    --column: cod_destino_prefeitura_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,56,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,56,13))
        END AS STRING
    ) AS id_prefeitura_destino,

    --column: cod_est_cadastral_atual_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS id_estado_cadastro_transferencia,
    --column: cod_est_cadastral_atual_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^1$') THEN 'Em Cadastramento'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^2$') THEN 'Sem Registro Civil'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^3$') THEN 'Cadastrado'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,1), r'^4$') THEN 'Excluido'
            ELSE TRIM(SUBSTRING(text,48,1))
        END AS STRING
    ) AS estado_cadastro_transferencia,

    --column: cod_familiar_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_antes_transferencia,

    --column: cod_munic_ibge_origem_2_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,2))
        END AS STRING
    ) AS id_uf_transferencia,

    --column: cod_munic_ibge_origem_5_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,51,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,51,5))
        END AS STRING
    ) AS id_municipio_transferencia,

    --column: dat_transferencia_famt
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_transferencia,

    --column: num_reg_arquivo_famt
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_transferencia,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0624'
    AND SUBSTRING(text,38,2) = '16'

