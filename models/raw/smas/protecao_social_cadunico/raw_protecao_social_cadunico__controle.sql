
{{
    config(
        alias='controle',
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

    --column: cod_versao_layout_hdr
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,5))
        END AS STRING
    ) AS versao_layout_arquivo,

    --column: dta_extracao_dados_hdr
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,8))
        END    ) AS data_extracao_dados,

    --column: dta_posicao_cadastro_hdr
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,8))
        END    ) AS data_posicao_cadastro,

    --column: nom_arquivo_hdr
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,30), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,30))
        END AS STRING
    ) AS nome_arquivo,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0601'
    AND SUBSTRING(text,38,2) = '00'

UNION ALL


SELECT

    --column: cod_versao_layout_hdr
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,5))
        END AS STRING
    ) AS versao_layout_arquivo,

    --column: dta_extracao_dados_hdr
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,8))
        END    ) AS data_extracao_dados,

    --column: dta_posicao_cadastro_hdr
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,8))
        END    ) AS data_posicao_cadastro,

    --column: nom_arquivo_hdr
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,30), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,30))
        END AS STRING
    ) AS nome_arquivo,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0603'
    AND SUBSTRING(text,38,2) = '00'

UNION ALL


SELECT

    --column: cod_versao_layout_hdr
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,5))
        END AS STRING
    ) AS versao_layout_arquivo,

    --column: dta_extracao_dados_hdr
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,8))
        END    ) AS data_extracao_dados,

    --column: dta_posicao_cadastro_hdr
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,8))
        END    ) AS data_posicao_cadastro,

    --column: nom_arquivo_hdr
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,30), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,30))
        END AS STRING
    ) AS nome_arquivo,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0604'
    AND SUBSTRING(text,38,2) = '00'

UNION ALL


SELECT

    --column: cod_versao_layout_hdr
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,5))
        END AS STRING
    ) AS versao_layout_arquivo,

    --column: dta_extracao_dados_hdr
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,8))
        END    ) AS data_extracao_dados,

    --column: dta_posicao_cadastro_hdr
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,8))
        END    ) AS data_posicao_cadastro,

    --column: nom_arquivo_hdr
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,30), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,30))
        END AS STRING
    ) AS nome_arquivo,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0609'
    AND SUBSTRING(text,38,2) = '00'

UNION ALL


SELECT

    --column: cod_versao_layout_hdr
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,5))
        END AS STRING
    ) AS versao_layout_arquivo,

    --column: dta_extracao_dados_hdr
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,8))
        END    ) AS data_extracao_dados,

    --column: dta_posicao_cadastro_hdr
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,8))
        END    ) AS data_posicao_cadastro,

    --column: nom_arquivo_hdr
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,30), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,30))
        END AS STRING
    ) AS nome_arquivo,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0612'
    AND SUBSTRING(text,38,2) = '00'

UNION ALL


SELECT

    --column: cod_versao_layout_hdr
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,5))
        END AS STRING
    ) AS versao_layout_arquivo,

    --column: dta_extracao_dados_hdr
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,8))
        END    ) AS data_extracao_dados,

    --column: dta_posicao_cadastro_hdr
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,8))
        END    ) AS data_posicao_cadastro,

    --column: nom_arquivo_hdr
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,30), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,30))
        END AS STRING
    ) AS nome_arquivo,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0615'
    AND SUBSTRING(text,38,2) = '00'

UNION ALL


SELECT

    --column: cod_versao_layout_hdr
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,5))
        END AS STRING
    ) AS versao_layout_arquivo,

    --column: dta_extracao_dados_hdr
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,8))
        END    ) AS data_extracao_dados,

    --column: dta_posicao_cadastro_hdr
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,8))
        END    ) AS data_posicao_cadastro,

    --column: nom_arquivo_hdr
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,30), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,30))
        END AS STRING
    ) AS nome_arquivo,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0617'
    AND SUBSTRING(text,38,2) = '00'

UNION ALL


SELECT

    --column: cod_versao_layout_hdr
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,5))
        END AS STRING
    ) AS versao_layout_arquivo,

    --column: dta_extracao_dados_hdr
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,8))
        END    ) AS data_extracao_dados,

    --column: dta_posicao_cadastro_hdr
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,8))
        END    ) AS data_posicao_cadastro,

    --column: nom_arquivo_hdr
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,30), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,30))
        END AS STRING
    ) AS nome_arquivo,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0619'
    AND SUBSTRING(text,38,2) = '00'

UNION ALL


SELECT

    --column: cod_versao_layout_hdr
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,5))
        END AS STRING
    ) AS versao_layout_arquivo,

    --column: dta_extracao_dados_hdr
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,8))
        END    ) AS data_extracao_dados,

    --column: dta_posicao_cadastro_hdr
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,8))
        END    ) AS data_posicao_cadastro,

    --column: nom_arquivo_hdr
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,30), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,30))
        END AS STRING
    ) AS nome_arquivo,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0620'
    AND SUBSTRING(text,38,2) = '00'

UNION ALL


SELECT

    --column: cod_versao_layout_hdr
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,5))
        END AS STRING
    ) AS versao_layout_arquivo,

    --column: dta_extracao_dados_hdr
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,8))
        END    ) AS data_extracao_dados,

    --column: dta_posicao_cadastro_hdr
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,8))
        END    ) AS data_posicao_cadastro,

    --column: nom_arquivo_hdr
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,30), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,30))
        END AS STRING
    ) AS nome_arquivo,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0623'
    AND SUBSTRING(text,38,2) = '00'

UNION ALL


SELECT

    --column: cod_versao_layout_hdr
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,70,5), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,70,5))
        END AS STRING
    ) AS versao_layout_arquivo,

    --column: dta_extracao_dados_hdr
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,83,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,83,8))
        END    ) AS data_extracao_dados,

    --column: dta_posicao_cadastro_hdr
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,75,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,75,8))
        END    ) AS data_posicao_cadastro,

    --column: nom_arquivo_hdr
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,30), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,30))
        END AS STRING
    ) AS nome_arquivo,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0624'
    AND SUBSTRING(text,38,2) = '00'

